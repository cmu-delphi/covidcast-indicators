# -*- coding: utf-8 -*-
"""Functions to call when running the nssp_ed indicator.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_nssp_ed`.  `run_module`'s lone
argument should be a nested dictionary of parameters loaded from the
params.json file.  We expect the `params` to have the following structure:
    - "common":
        - "export_dir": str, directory to write daily output
        - "backup_dir": str, directory where raw pulled data is archived
        - "log_filename": (optional) str, path to log file
        - "log_exceptions" (optional): bool, whether to log exceptions to file
        - "custom_run" (optional): bool, True for patch runs (skips the live
          pull; data must be supplied via `source_df`)
    - "indicator": (optional)
        - "wip_signal": (optional) Any[str, bool], list of signals that are
            works in progress, or True if all signals in the registry are works
            in progress, or False if only unpublished signals are.  See
            `delphi_utils.add_prefix()`
        - "socrata_token" (optional): str, optional Socrata app token. The feed
          is public; a token only raises the API rate limit.
        - "export_start_date" (optional): str, YYYY-MM-DD, earliest reference
          date to export. Defaults to the start of the feed.
"""

import time
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
import us
from delphi_utils import create_export_csv, get_structured_logger
from delphi_utils.geomap import GeoMapper
from delphi_utils.nancodes import add_default_nancodes

from .constants import (
    AUXILIARY_COLS,
    CSV_COLS,
    GEOS,
    NATIONAL_LABEL,
    SIGNALS,
    SIGNALS_BASE,
    SMOOTHED_SUFFIX,
    SMOOTHING_WINDOW_DAYS,
)
from .pull import pull_nssp_ed_data


def add_needed_columns(df, col_names=None):
    """Short util to add expected columns not found in the dataset."""
    if col_names is None:
        col_names = AUXILIARY_COLS

    for col_name in col_names:
        df[col_name] = np.nan
    df = add_default_nancodes(df)
    return df


def add_smoothed_signals(df: pd.DataFrame) -> pd.DataFrame:
    """Add trailing 7-day mean variants of every base signal.

    The smoothed signal for day ``t`` is the mean of the base signal over days
    ``t-6..t`` within each geography. A full 7-day window is required
    (``min_periods=7``), so the first 6 days of each geo series are NaN and are
    filtered out before export.
    """
    df = df.copy()
    for signal in SIGNALS_BASE:
        smoothed = signal + SMOOTHED_SUFFIX
        df[smoothed] = (
            df.sort_values("timestamp")
            .groupby("geography")[signal]
            .transform(lambda s: s.rolling(SMOOTHING_WINDOW_DAYS, min_periods=SMOOTHING_WINDOW_DAYS).mean())
        )
    return df


def map_state_geocode(state_name: str, logger=None) -> Optional[str]:
    """Map a feed geography label to a covidcast state geo_id.

    Returns the lowercase postal abbreviation (e.g. "ca"). The ``us`` lookup
    does not resolve "District of Columbia" by name (this is also why the
    ``nssp`` indicator special-cases it), so DC is mapped explicitly.
    Returns None for any other unknown label; the row is then dropped with a
    warning instead of being misattributed.
    """
    if state_name == "District of Columbia":
        return "dc"
    state = us.states.lookup(state_name)
    if state is None:
        if logger is not None:
            logger.warning("Unknown state geography in feed; dropping rows", geography=state_name)
        return None
    return state.abbr.lower()


def logging(start_time, run_stats, logger):
    """Boilerplate making logs."""
    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    min_max_date = run_stats and min(s[0] for s in run_stats)
    csv_export_count = sum(s[-1] for s in run_stats)
    max_lag_in_days = min_max_date and (datetime.now() - min_max_date).days
    formatted_min_max_date = min_max_date and min_max_date.strftime("%Y-%m-%d")
    logger.info(
        "Completed indicator run",
        elapsed_time_in_seconds=elapsed_time_in_seconds,
        csv_export_count=csv_export_count,
        max_lag_in_days=max_lag_in_days,
        oldest_final_export_date=formatted_min_max_date,
    )


def run_module(params, logger=None, source_df: Optional[pd.DataFrame] = None):
    """Run the indicator.

    Arguments
    --------
    params:  Dict[str, Any]
        Nested dictionary of parameters.
    logger:
        Structured logger. If None, one is created (normal indicator run).
    source_df: Optional[pd.DataFrame]
        Pre-pulled raw feed data in the feed's long format. Used by patch
        runs (custom_run=True) instead of hitting the live API.
    """
    start_time = time.time()
    custom_run = params["common"].get("custom_run", False)
    # logger doesn't exist yet means run_module is called from normal indicator run (instead of any custom run)
    if not logger:
        logger = get_structured_logger(
            __name__,
            filename=params["common"].get("log_filename"),
            log_exceptions=params["common"].get("log_exceptions", True),
        )
        if custom_run:
            logger.warning("custom_run flag is on despite direct indicator run call. Normal indicator run continues.")
            custom_run = False
    export_dir = params["common"]["export_dir"]
    backup_dir = params["common"]["backup_dir"]
    socrata_token = params.get("indicator", {}).get("socrata_token") or None
    export_start_date = params.get("indicator", {}).get("export_start_date", "2022-09-25")
    run_stats = []

    logger.info("Generating NSSP ED signals")
    df_pull = pull_nssp_ed_data(
        backup_dir,
        custom_run=custom_run,
        socrata_token=socrata_token,
        source_df=source_df,
        logger=logger,
    )
    if df_pull.empty:
        logger.error("No data pulled; nothing to export")
        return

    # Data-quality check: warn if the feed looks stale. The feed is refreshed
    # weekly, so the newest record should be at most ~2 weeks old.
    max_timestamp = pd.to_datetime(df_pull["timestamp"]).max()
    data_lag_days = (datetime.now() - max_timestamp).days
    if data_lag_days > 14:
        logger.warning(
            "Feed data looks stale",
            newest_record=max_timestamp.strftime("%Y-%m-%d"),
            lag_days=data_lag_days,
        )

    # Restrict the export window. The full feed history is pulled every run so
    # the smoothed signals are correct; only dates >= export_start_date are
    # written out. (The filter is applied after smoothing so the trailing
    # window is not starved at the start of the export window.)
    df_pull = add_smoothed_signals(df_pull)
    df_pull = df_pull[pd.to_datetime(df_pull["timestamp"]) >= pd.to_datetime(export_start_date)]

    geo_mapper = GeoMapper()
    for signal in SIGNALS:
        for geo in GEOS:
            df = df_pull[["timestamp", "geography", signal]].copy()
            df["val"] = df[signal]
            logger.info("Generating signal and exporting to CSV", geo_type=geo, signal=signal)
            if geo == "nation":
                # The feed reports the national aggregate directly; use it
                # as-is instead of re-aggregating states.
                df = df[df["geography"] == NATIONAL_LABEL]
                df["geo_id"] = "us"
            elif geo == "state":
                df = df[df["geography"] != NATIONAL_LABEL]
                df["geo_id"] = df["geography"].apply(lambda x: map_state_geocode(x, logger))
                df = df[df["geo_id"].notnull()]
            elif geo == "hhs":
                # Population-weighted mean of the state percentages, the same
                # approach the nssp indicator uses for HHS. States missing
                # from the feed on a given day are extrapolated from the
                # reporting states (see GeoMapper.aggregate_by_weighted_sum).
                df = df[df["geography"] != NATIONAL_LABEL]
                df = df[["geography", "val", "timestamp"]]
                df = geo_mapper.add_population_column(df, geocode_type="state_name", geocode_col="geography")
                df = geo_mapper.add_geocode(df, "state_name", "state_code", from_col="geography")
                df = geo_mapper.add_geocode(df, "state_code", "hhs", from_col="state_code", new_col="geo_id")
                df = geo_mapper.aggregate_by_weighted_sum(df, "geo_id", "val", "timestamp", "population")
                df = df.rename(columns={"weighted_val": "val"})
            # add se, sample_size, and na codes. The feed publishes percentages
            # only (no standard errors or sample sizes), so these stay missing
            # with the standard missing-value codes.
            missing_cols = set(CSV_COLS) - set(df.columns)
            df = add_needed_columns(df, col_names=list(missing_cols))
            df_csv = df[CSV_COLS + ["timestamp"]]

            # remove rows with missing values (e.g. the leading edge of the
            # smoothed signals, or pathogens temporarily absent from the feed)
            df_csv = df_csv[df_csv["val"].notnull()]
            if df_csv.empty:
                logger.warning("No data for signal and geo combination", signal=signal, geo=geo)
                continue

            # actual export; the feed is daily grain
            dates = create_export_csv(
                df_csv,
                export_dir=export_dir,
                geo_res=geo,
                sensor=signal,
                weekly_dates=False,
            )
            if len(dates) > 0:
                run_stats.append((max(dates), len(dates)))

    ## log this indicator run
    logging(start_time, run_stats, logger)
