# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_claims_hosp`.
"""

# standard packages
import time
from datetime import datetime, timedelta
from pathlib import Path

# third party
from delphi_utils import get_structured_logger

# first party
from .config import Config
from .update_indicator import ClaimsHospIndicatorUpdater


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
            - "input_file": str, optional filenames to download. If null,
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

    # handle range of estimates to produce
    # filename expected to have format: EDI_AGG_INPATIENT_DDMMYYYY_HHMM{timezone}.csv.gz
    if params["indicator"]["drop_date"] is None:
        dropdate_dt = datetime.strptime(
            Path(params["indicator"]["input_file"]).name.split("_")[3], "%d%m%Y")
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

    # generate indicator csvs
    for geo in params["indicator"]["geos"]:
        for weekday in params["indicator"]["weekday"]:
            if weekday:
                logger.info("starting weekday adj", geo = geo)
            else:
                logger.info("starting no weekday adj", geo =  geo)

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
                signal_name
            )
            updater.update_indicator(params["indicator"]["input_file"],
                                     params["common"]["export_dir"])
        logger.info("finished updating", geo = geo)

    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    logger.info("Completed indicator run",
        elapsed_time_in_seconds = elapsed_time_in_seconds)
