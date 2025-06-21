# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_backfill_alerting`.
"""

# standard packages
import time
from datetime import datetime, timedelta
from typing import Dict, Any

#  third party
from delphi_utils import get_structured_logger

# first party
from .download_ftp_files import download_covid
from .backfill import (generate_pivot_df, generate_backfill_df,
                       add_value_raw_and_7dav)
from .model import (model_traning_and_testing, evaluation)
from .config import Config

def update_alert_info(alert_info, new_alerts, count_type, geo,
                      bv_type, ref_lag):
    """Update the backfill alerting message.

    Parameters
    ----------
    alert_info : str, the backfill alerting message
    new_alerts : list, new alerts generated from models
    count_type: "Covid" or "Denom"
    geo: geographic unit
    bv_type: the type of the backfill variable
    ref_lag: k for change rate or anchor_lag for backfill fraction

    Returns
    -------
    str, updated backfill alerting message.
    """
    head = ", ".join([Config.bv_names[(bv_type, ref_lag)],
                      Config.count_names[count_type],
                      f"at {Config.geo_names[geo]} level"]) + ":\n"
    alert_info += head
    if len(new_alerts[0]) > 0:
        alert_info += ("Excessive Updates: " \
                       + ', '.join(new_alerts[0])+ "\n")
    if len(new_alerts[2]) > 0:
        alert_info += ("Excessive Updates in details: " \
                       + ', '.join(new_alerts[2]) + "\n")
    if len(new_alerts[1]) > 0:
        alert_info += ("Deficient Updates: " \
                       + ', '.join(new_alerts[1]) + "\n")
    if len(new_alerts[3]) > 0:
        alert_info += ("Deficient Updates in details: " \
                       + ', '.join(new_alerts[3]) + "\n")
    return alert_info

def run_module(params: Dict[str, Dict[str, Any]]):
    """
    Run the delphi_backfill_alerting module.

    Parameters
    ----------
    params
        Dictionary containing indicator configuration. Expected to have the following structure:
        - "common":
            - "export_dir": str, directory to write output.
            - "log_exceptions" (optional): bool, whether to log exceptions to file.
            - "log_filename" (optional): str, name of file to write logs
        - "indicator":
            - "input_cache_dir": str, directory to download source files.
            - "data_cahce_dir": str, directory to support data files.
            - "drop_date": str or null, YYYY-MM-DD format, date data is dropped. If set to
               null, current day minus 40 hours is used.
            - "n_backfill_days": int, number of past days to consider.
            - "se": bool, whether to write out standard errors.
            - "geos": list of str, geographies to consider.
            - "wip_signal": list of str or bool, to be passed to delphi_utils.add_prefix.
            - "ftp_conn": dict, connection information for source FTP.
    """
    start_time = time.time()

    logger = get_structured_logger(
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))

    if params["indicator"]["drop_date"] is None:
        # files are dropped about 4pm the day after the issue date
        dropdate = (datetime.now() - timedelta(days=1,hours=16))
        dropdate = dropdate.replace(hour=0,minute=0,second=0,microsecond=0)
    else:
        dropdate = datetime.strptime(params["indicator"]["drop_date"], "%Y-%m-%d")

    # Download the most recent file from the SFTP server
    download_covid(dropdate, params["indicator"]["input_cache_dir"],
                   params["indicator"]["ftp_conn"])

    training_mode = params["indicator"]["training_mode"] or (dropdate.day == 1)

    alerting_info = ""

    for count_type in Config.COUNT_TYPES: # Count for COVID or all the claims
        pivot_dfs = generate_pivot_df(
            Config.FILE_PATH, params["indicator"]["input_cache_dir"],
            params["indicator"]["data_cache_dir"],
            count_type, dropdate)
        for geo in Config.GEO_LEVELS: # County or State
            support_df = add_value_raw_and_7dav(pivot_dfs[geo])
            ### For Change Rates, Run daily
            for k in Config.BACKFILL_REF_LAG[Config.CHANGE_RATE]:# 1 or 7
                cr_data = generate_backfill_df(
                    pivot_dfs[geo], support_df, dropdate,
                    Config.CHANGE_RATE, ref_lag=k)
                results = model_traning_and_testing(
                    cr_data, dropdate, Config.CHANGE_RATE, k)
                alerts = evaluation(results, dropdate,
                                     params["indicator"]["result_cache_dir"])
                alerting_info = update_alert_info(alerting_info, alerts,
                                                  count_type, geo,
                                                  Config.CHANGE_RATE, k)

            ### For backfill fraction: run monthly
            for anchor_lag in Config.BACKFILL_REF_LAG[Config.BACKFILL_FRACTION]:
                if training_mode: # otherwise, don't run
                    frc_data = generate_backfill_df(
                        pivot_dfs[geo], support_df, dropdate,
                        Config.BACKFILL_FRACTION, ref_lag=anchor_lag)
                    # Don't need to save the trained model for backfill fraction
                    # Data for the most recent two months is tested.
                    results = model_traning_and_testing(
                        frc_data, dropdate, Config.CHANGE_RATE, k)
                    alerts = evaluation(results, dropdate,
                                     params["indicator"]["result_cache_dir"])
                    alerting_info = update_alert_info(alerting_info, alerts,
                                                      count_type, geo,
                                                      Config.BACKFILL_FRACTION,
                                                      anchor_lag)
    logger.critical(event="CHC Backfill Alerting run successful",
                    data_source="CHC Outpatient Counts",
                    alerting_info=alerting_info)

    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    logger.info("Completed indicator run",
                elapsed_time_in_seconds=elapsed_time_in_seconds)
