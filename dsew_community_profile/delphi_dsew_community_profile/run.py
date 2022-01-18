# -*- coding: utf-8 -*-
"""Functions to call when running the indicator.

This module should contain a function called `run_module`, that is executed when
the module is run with `python -m delphi_dsew_community_profile`.
`run_module`'s lone argument should be a nested dictionary of parameters loaded
from the params.json file.  We expect the `params` to have the following
structure:

    - "common":
        - "export_dir": str, directory to which the results are exported
        - "log_filename": (optional) str, path to log file
    - "indicator": (optional)
        - Any other indicator-specific settings
"""
from datetime import datetime
import time

from delphi_utils import get_structured_logger
from delphi_utils.export import create_export_csv
import pandas as pd

from .constants import make_signal_name
from .pull import fetch_new_reports


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
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))
    def replace_date_param(p):
        if p in params["indicator"] and params["indicator"][p] is not None:
            date_param = datetime.strptime(params["indicator"][p], "%Y-%m-%d").date()
            params["indicator"][p] = date_param
    replace_date_param("export_start_date")
    replace_date_param("export_end_date")
    export_params = {
        'start_date': params["indicator"].get("export_start_date", None),
        'end_date': params["indicator"].get("export_end_date", None)
    }
    export_params = {
        k: pd.to_datetime(v) if v is not None else v
        for k, v in export_params.items()
    }

    run_stats = []
    dfs = fetch_new_reports(params, logger)
    for key, df in dfs.items():
        (geo, sig) = key
        dates = create_export_csv(
            df,
            params['common']['export_dir'],
            geo,
            make_signal_name(sig),
            **export_params
        )
        if len(dates)>0:
            run_stats.append((max(dates), len(dates)))

    ## log this indicator run
    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    min_max_date = run_stats and min(s[0] for s in run_stats)
    csv_export_count = sum(s[-1] for s in run_stats)
    max_lag_in_days = min_max_date and (datetime.now() - min_max_date).days
    formatted_min_max_date = min_max_date and min_max_date.strftime("%Y-%m-%d")
    logger.info("Completed indicator run",
                elapsed_time_in_seconds = elapsed_time_in_seconds,
                csv_export_count = csv_export_count,
                max_lag_in_days = max_lag_in_days,
                oldest_final_export_date = formatted_min_max_date)
