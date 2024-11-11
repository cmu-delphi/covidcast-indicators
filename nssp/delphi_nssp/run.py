# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_nssp`.  `run_module`'s lone argument should be a
nested dictionary of parameters loaded from the params.json file.  We expect the `params` to have
the following structure:
    - "common":
        - "export_dir": str, directory to write daily output
        - "log_filename": (optional) str, path to log file
        - "log_exceptions" (optional): bool, whether to log exceptions to file
    - "indicator": (optional)
        - "wip_signal": (optional) Any[str, bool], list of signals that are
            works in progress, or True if all signals in the registry are works
            in progress, or False if only unpublished signals are.  See
            `delphi_utils.add_prefix()`
        - "test_file" (optional): str, name of file from which to read test data
        - "socrata_token": str, authentication for upstream data pull
    - "archive" (optional): if provided, output will be archived with S3
        - "aws_credentials": Dict[str, str], AWS login credentials (see S3 documentation)
        - "bucket_name: str, name of S3 bucket to read/write
        - "cache_dir": str, directory of locally cached data
"""

import time
from datetime import datetime

import numpy as np
import us
from delphi_utils import create_export_csv, get_structured_logger
from delphi_utils.geomap import GeoMapper
from delphi_utils.nancodes import add_default_nancodes

from .constants import AUXILIARY_COLS, CSV_COLS, GEOS, SECONDARY_GEOS, SECONDARY_SIGNALS, SIGNALS
from .pull import pull_nssp_data, secondary_pull_nssp_data


def add_needed_columns(df, col_names=None):
    """Short util to add expected columns not found in the dataset."""
    if col_names is None:
        col_names = AUXILIARY_COLS

    for col_name in col_names:
        df[col_name] = np.nan
    df = add_default_nancodes(df)
    return df


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


def run_module(params):
    """
    Run the indicator.

    Arguments
    --------
    params:  Dict[str, Any]
        Nested dictionary of parameters.
    """
    start_time = time.time()
    logger = get_structured_logger(
        __name__,
        filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True),
    )
    export_dir = params["common"]["export_dir"]
    backup_dir = params["common"]["backup_dir"]
    custom_run = params["common"].get("custom_run", False)
    socrata_token = params["indicator"]["socrata_token"]

    run_stats = []

    logger.info("Generating primary signals")
    ## build the base version of the signal at the most detailed geo level you can get.
    ## compute stuff here or farm out to another function or file
    df_pull = pull_nssp_data(socrata_token, backup_dir, custom_run=custom_run, logger=logger)
    ## aggregate
    geo_mapper = GeoMapper()
    for signal in SIGNALS:
        for geo in GEOS:
            df = df_pull.copy()
            df["val"] = df[signal]
            logger.info("Generating signal and exporting to CSV", geo_type=geo, signal=signal)
            if geo == "nation":
                df = df[df["geography"] == "United States"]
                df["geo_id"] = "us"
            elif geo == "state":
                df = df[(df["county"] == "All") & (df["geography"] != "United States")]
                df["geo_id"] = df["geography"].apply(
                    lambda x: us.states.lookup(x).abbr.lower() if us.states.lookup(x) else "dc"
                )
            elif geo == "hrr":
                df = df[["fips", "val", "timestamp"]]
                # fips -> hrr has a weighted version
                df = geo_mapper.replace_geocode(df, "fips", "hrr")
                df = df.rename(columns={"hrr": "geo_id"})
            elif geo == "msa":
                df = df[["fips", "val", "timestamp"]]
                # fips -> msa doesn't have a weighted version, so we need to add columns and sum ourselves
                df = geo_mapper.add_population_column(df, geocode_type="fips", geocode_col="fips")
                df = geo_mapper.add_geocode(df, "fips", "msa", from_col="fips", new_col="geo_id")
                df = geo_mapper.aggregate_by_weighted_sum(df, "geo_id", "val", "timestamp", "population")
                df = df.rename(columns={"weighted_val": "val"})
            elif geo == "hhs":
                df = df[(df["county"] == "All") & (df["geography"] != "United States")]
                df = df[["geography", "val", "timestamp"]]
                df = geo_mapper.add_population_column(df, geocode_type="state_name", geocode_col="geography")
                df = geo_mapper.add_geocode(df, "state_name", "state_code", from_col="state_name")
                df = geo_mapper.add_geocode(df, "state_code", "hhs", from_col="state_code", new_col="geo_id")
                df = geo_mapper.aggregate_by_weighted_sum(df, "geo_id", "val", "timestamp", "population")
                df = df.rename(columns={"weighted_val": "val"})
            else:
                df = df[df["county"] != "All"]
                df["geo_id"] = df["fips"]
            # add se, sample_size, and na codes
            missing_cols = set(CSV_COLS) - set(df.columns)
            df = add_needed_columns(df, col_names=list(missing_cols))
            df_csv = df[CSV_COLS + ["timestamp"]]
            # actual export
            dates = create_export_csv(
                df_csv,
                geo_res=geo,
                export_dir=export_dir,
                sensor=signal,
                weekly_dates=True,
            )
            if len(dates) > 0:
                run_stats.append((max(dates), len(dates)))

    logger.info("Generating secondary signals")
    secondary_df_pull = secondary_pull_nssp_data(socrata_token)
    for signal in SECONDARY_SIGNALS:
        secondary_df_pull_signal = secondary_df_pull[secondary_df_pull["signal"] == signal]
        if secondary_df_pull_signal.empty:
            logger.warning("No data found for signal", signal=signal)
            continue
        for geo in SECONDARY_GEOS:
            df = secondary_df_pull_signal.copy()
            logger.info("Generating signal and exporting to CSV", geo_type=geo, signal=signal)
            if geo == "state":
                df = df[(df["geo_type"] == "state")]
                df["geo_id"] = df["geo_value"].apply(
                    lambda x: (
                        us.states.lookup(x).abbr.lower()
                        if us.states.lookup(x)
                        else ("dc" if x == "District of Columbia" else x)
                    )
                )
                unexpected_state_names = df[df["geo_id"] == df["geo_value"]]
                if unexpected_state_names.shape[0] > 0:
                    logger.error(
                        "Unexpected state names",
                        unexpected_state_names=unexpected_state_names["geo_value"].unique(),
                    )
                    raise RuntimeError
            elif geo == "nation":
                df = df[(df["geo_type"] == "nation")]
                df["geo_id"] = "us"
            elif geo == "hhs":
                df = df[(df["geo_type"] == "hhs")]
                df["geo_id"] = df["geo_value"]
            # add se, sample_size, and na codes
            missing_cols = set(CSV_COLS) - set(df.columns)
            df = add_needed_columns(df, col_names=list(missing_cols))
            df_csv = df[CSV_COLS + ["timestamp"]]
            # actual export
            dates = create_export_csv(
                df_csv,
                geo_res=geo,
                export_dir=export_dir,
                sensor=signal,
                weekly_dates=True,
            )
            if len(dates) > 0:
                run_stats.append((max(dates), len(dates)))

    ## log this indicator run
    logging(start_time, run_stats, logger)
