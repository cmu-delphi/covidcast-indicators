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
import covidcast

from .constants import make_signal_name, SIGNALS
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
        if p in params["indicator"]:
            if params["indicator"][p] is None:
                del params["indicator"][p]
            else:
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
        (geo, sig, is_prop) = key
        if sig not in params["indicator"]["export_signals"]:
            continue
        dates = create_export_csv(
            df,
            params['common']['export_dir'],
            geo,
            make_signal_name(sig, is_prop),
            **export_params
        )
        if len(dates)>0:
            run_stats.append((max(dates), len(dates)))

    ## If any requested signal is not in metadata, generate it for all dates.
    #
    # Only do so if params.reports is set to "new". If set to "all", the
    # previous fetch_new_reports + CSV loop will already have generated the full
    # history for new signals. If params.reports is set to a specific date
    # range, that request overrides automated backfill.
    if params['indicator']['reports'] == 'new':
        # Fetch metadata to check how recent signals are
        metadata = covidcast.metadata()
        sensor_names = {
            SIGNALS[key][name_field]: key
            for key in params["indicator"]["export_signals"]
            for name_field in ["api_name", "api_prop_name"]
            if name_field in SIGNALS[key].keys()
        }

        # Filter to only those we currently want to produce
        cpr_metadata = metadata[(metadata.data_source == "dsew-cpr") &
            (metadata.signal.isin(sensor_names.keys()))]

        new_signals = set(sensor_names.keys()).difference(set(cpr_metadata.signal))
        if new_signals:
            # If any signal not in metadata yet, we need to backfill its full
            # history.
            params['indicator']['reports'] = 'all'
            params['indicator']['export_signals'] = {sensor_names[key] for key in new_signals}

            dfs = fetch_new_reports(params, logger)
            for key, df in dfs.items():
                (geo, sig, is_prop) = key
                if sig not in params["indicator"]["export_signals"]:
                    continue
                dates = create_export_csv(
                    df,
                    params['common']['export_dir'],
                    geo,
                    make_signal_name(sig, is_prop),
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
    # Print warning if no CSV export
    if csv_export_count == 0:
        logger.warning("No CSV output - manual validation may be needed")
