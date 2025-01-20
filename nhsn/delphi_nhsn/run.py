# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.  `run_module`'s lone argument should be a
nested dictionary of parameters loaded from the params.json file.  We expect the `params` to have
the following structure:
    - "common":
        - "export_dir": str, directory to which the results are exported
        - "log_filename": (optional) str, path to log file
    - "indicator": (optional)
        - "wip_signal": (optional) Any[str, bool], list of signals that are works in progress, or
            True if all signals in the registry are works in progress, or False if only
            unpublished signals are.  See `delphi_utils.add_prefix()`
        - Any other indicator-specific settings
"""
import time
from datetime import date, datetime, timedelta

import numpy as np
from delphi_utils import GeoMapper, get_structured_logger
from delphi_utils.export import create_export_csv

from .constants import GEOS, PRELIM_SIGNALS_MAP, SIGNALS_MAP
from .pull import pull_nhsn_data, pull_preliminary_nhsn_data


def run_module(params, logger=None):
    """
    Run the indicator.

    Arguments
    --------
    params:  Dict[str, Any]
        Nested dictionary of parameters.
    """
    start_time = time.time()
    if not logger:
        logger = get_structured_logger(
            __name__,
            filename=params["common"].get("log_filename"),
            log_exceptions=params["common"].get("log_exceptions", True),
        )
    export_dir = params["common"]["export_dir"]
    backup_dir = params["common"]["backup_dir"]
    custom_run = params["common"].get("custom_run", False)
    issue_date = params.get("patch", dict()).get("issue_date", None)
    socrata_token = params["indicator"]["socrata_token"]
    export_start_date = params["indicator"]["export_start_date"]
    run_stats = []

    if export_start_date == "latest":  # Find the previous Saturday
        export_start_date = date.today() - timedelta(days=date.today().weekday() + 2)
        export_start_date = export_start_date.strftime("%Y-%m-%d")

    nhsn_df = pull_nhsn_data(socrata_token, backup_dir, custom_run=custom_run, issue_date=issue_date, logger=logger)
    preliminary_nhsn_df = pull_preliminary_nhsn_data(
        socrata_token, backup_dir, custom_run=custom_run, issue_date=issue_date, logger=logger
    )

    geo_mapper = GeoMapper()
    signal_df_dict = {signal: nhsn_df for signal in SIGNALS_MAP}
    # some of the source backups do not include for preliminary data TODO remove after first patch
    if not preliminary_nhsn_df.empty:
        signal_df_dict.update({signal: preliminary_nhsn_df for signal in PRELIM_SIGNALS_MAP})

    for signal, df_pull in signal_df_dict.items():
        for geo in GEOS:
            df = df_pull.copy()
            df = df[["timestamp", "geo_id", signal]]
            df.rename({signal: "val"}, axis=1, inplace=True)
            if geo == "nation":
                df = df[df["geo_id"] == "us"]
            elif geo == "hhs":
                df = df[df["geo_id"] != "us"]
                df.rename(columns={"geo_id": "state_id"}, inplace=True)
                df = geo_mapper.add_geocode(df, "state_id", "state_code", from_col="state_id")
                df = geo_mapper.add_geocode(df, "state_code", "hhs", from_col="state_code", new_col="hhs")
                df = geo_mapper.replace_geocode(
                    df, from_col="state_code", from_code="state_code", new_col="geo_id", new_code="hhs"
                )
            elif geo == "state":
                df = df[df_pull["geo_id"] != "us"]
                df = df[df['geo_id'].str.len() == 2] # hhs region is a value in geo_id column

            df["se"] = np.nan
            df["sample_size"] = np.nan
            dates = create_export_csv(
                df,
                geo_res=geo,
                export_dir=export_dir,
                start_date=datetime.strptime(export_start_date, "%Y-%m-%d"),
                sensor=signal,
                weekly_dates=True,
            )
            if len(dates) > 0:
                run_stats.append((max(dates), len(dates)))

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
