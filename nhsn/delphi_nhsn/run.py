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
from datetime import timedelta, datetime, date
from itertools import product

import numpy as np
import pandas as pd
from delphi_utils import get_structured_logger
from delphi_utils.export import create_export_csv
from delphi_utils.geomap import GeoMapper

from .constants import GEOS, ALL_SIGNALS
from .pull import pull_nhsn_data, pull_preliminary_nhsn_data


def run_module(params):
    """
    Runs the indicator

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
    export_start_date = params["indicator"]["export_start_date"]
    run_stats = []

    if export_start_date == "latest":  # Find the previous Saturday
        export_start_date = date.today() - timedelta(
            days=date.today().weekday() + 2)
        export_start_date = export_start_date.strftime('%Y-%m-%d')

    nhsn_df = pull_nhsn_data(socrata_token, backup_dir, custom_run=custom_run, logger=logger)
    preliminary_nhsn_df = pull_preliminary_nhsn_data(socrata_token, backup_dir, custom_run=custom_run, logger=logger)

    df_pull = pd.concat([nhsn_df, preliminary_nhsn_df])

    nation_df = df_pull[df_pull["geo_id"] == "USA"]
    state_df = df_pull[df_pull["geo_id"] != "USA"]


    if not df_pull.empty:
        for geo in GEOS:
            if geo == "nation":
                df = nation_df
            else:
                df = state_df
            for signal in ALL_SIGNALS:
                df["val"] = df[signal]
                df["se"] = np.nan
                df["sample_size"] = np.nan
                dates = create_export_csv(
                    df,
                    geo_res=geo,
                    export_dir=export_dir,
                    start_date=datetime.strptime(export_start_date, "%Y-%m-%d"),
                    sensor=signal,
                    weekly_dates=True
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

