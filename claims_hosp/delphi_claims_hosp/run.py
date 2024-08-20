# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_claims_hosp`.
"""

# standard packages
import time
import os
from datetime import datetime, timedelta
from pathlib import Path

# third party
from delphi_utils import get_structured_logger

# first party
from .config import Config
from .download_claims_ftp_files import download
from .modify_claims_drops import modify_and_write
from .get_latest_claims_name import get_latest_filename
from .update_indicator import ClaimsHospIndicatorUpdater
from .backfill import (store_backfill_file, merge_backfill_file)


def run_module(params):
    """
    Generate updated claims-based hospitalization indicator values.

    Parameters
    ----------
    params
        Dictionary containing indicator configuration. Expected to have the following structure:
        - "common":
            - "export_dir": str, directory to write output.
            - "log_exceptions" (optional): bool, whether to log exceptions to file.
            - "log_filename" (optional): str, name of file to write logs
        - "indicator":
            - "input_dir": str, directory to downloaded raw files. If null,
                defaults are set in retrieve_files().
            - "start_date": str, YYYY-MM-DD format, first day to generate data for.
            - "end_date": str or null, YYYY-MM-DD format, last day to generate data for.
               If set to null, end date is derived from drop date and n_waiting_days.
            - "drop_date": str or null, YYYY-MM-DD format, date data is dropped. If set to
               null, current day minus 40 hours is used.
            - "n_backfill_days": int, number of past days to generate estimates for.
            - "n_waiting_days": int, number of most recent days to skip estimates for.
            - "write_se": bool, whether to write out standard errors.
            - "obfuscated_prefix": str, prefix for signal name if write_se is True.
            - "parallel": bool, whether to update sensor in parallel.
            - "geos": list of str, geographies to generate sensor for.
            - "weekday": list of bool, which weekday adjustments to perform. For each value in the
                list, signals will be generated with weekday adjustments (True) or without
                adjustments (False).
    """
    start_time = time.time()
    logger = get_structured_logger(
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))

    # pull latest data
    download(params["indicator"]["ftp_credentials"],
             params["indicator"]["input_dir"], logger)

    # aggregate data
    modify_and_write(params["indicator"]["input_dir"], logger)

    # find the latest files (these have timestamps)
    claims_file = get_latest_filename(params["indicator"]["input_dir"], logger)

    # handle range of estimates to produce
    # filename expected to have format: EDI_AGG_INPATIENT_DDMMYYYY_HHMM{timezone}.csv.gz
    if params["indicator"]["drop_date"] is None:
        dropdate_dt = datetime.strptime(
            Path(claims_file).name.split("_")[3], "%d%m%Y")
    else:
        dropdate_dt = datetime.strptime(params["indicator"]["drop_date"], "%Y-%m-%d")

    # produce estimates for n_backfill_days
    # most recent n_waiting_days won't be est
    enddate_dt = dropdate_dt - timedelta(days=params["indicator"]["n_waiting_days"])
    startdate_dt = enddate_dt - timedelta(days=params["indicator"]["n_backfill_days"])
    enddate = str(enddate_dt.date())
    startdate = str(startdate_dt.date())
    dropdate = str(dropdate_dt.date())

    # now allow manual overrides
    if params["indicator"]["end_date"] is not None:
        enddate = params["indicator"]["end_date"]
    if params["indicator"]["start_date"] is not None:
        startdate = params["indicator"]['start_date']

    # Store backfill data
    if params["indicator"].get("generate_backfill_files", True):
        backfill_dir = params["indicator"]["backfill_dir"]
        backfill_merge_day = params["indicator"]["backfill_merge_day"]
        merge_backfill_file(backfill_dir, backfill_merge_day, datetime.today())
        store_backfill_file(claims_file, dropdate_dt, backfill_dir)

    # print out information
    logger.info("Loaded params",
                startdate = startdate,
                enddate = enddate,
                dropdate = dropdate,
                n_backfill_days = params["indicator"]["n_backfill_days"],
                n_waiting_days = params["indicator"]["n_waiting_days"],
                geos = params["indicator"]["geos"],
                outpath = params["common"]["export_dir"],
                parallel = params["indicator"]["parallel"],
                weekday = params["indicator"]["weekday"],
                write_se = params["indicator"]["write_se"])

    max_dates = []
    n_csv_export = []
    # generate indicator csvs
    for geo in params["indicator"]["geos"]:
        for weekday in params["indicator"]["weekday"]:
            if weekday:
                logger.info("Starting weekday adj", geo=geo)
            else:
                logger.info("Starting no weekday adj", geo=geo)

            signal_name = Config.signal_weekday_name if weekday else Config.signal_name
            if params["indicator"]["write_se"]:
                assert params["indicator"]["obfuscated_prefix"] is not None, \
                    "supply obfuscated prefix in params.json"
                signal_name = params["indicator"]["obfuscated_prefix"] + "_" + signal_name

            logger.info("Updating signal name", signal_name = signal_name)
            updater = ClaimsHospIndicatorUpdater(
                startdate,
                enddate,
                dropdate,
                geo,
                params["indicator"]["parallel"],
                weekday,
                params["indicator"]["write_se"],
                signal_name,
                logger,
            )
            updater.update_indicator(
                claims_file,
                params["common"]["export_dir"],
            )
            max_dates.append(updater.output_dates[-1])
            n_csv_export.append(len(updater.output_dates))
        logger.info("Finished updating", geo=geo)

    # Remove all the raw files
    for fn in os.listdir(params["indicator"]["input_dir"]):
        if ".csv.gz" in fn:
            os.remove(f'{params["indicator"]["input_dir"]}/{fn}')
    logger.info('Remove all the raw files.')

    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    min_max_date = min(max_dates)
    max_lag_in_days = (datetime.now() - min_max_date).days
    csv_export_count = sum(n_csv_export)
    formatted_min_max_date = min_max_date.strftime("%Y-%m-%d")
    logger.info("Completed indicator run",
                elapsed_time_in_seconds = elapsed_time_in_seconds,
                csv_export_count = csv_export_count,
                max_lag_in_days = max_lag_in_days,
                oldest_final_export_date = formatted_min_max_date)
