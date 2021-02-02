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
from delphi_utils import read_params, get_structured_logger

# first party
from .config import Config
from .update_indicator import ClaimsHospIndicatorUpdater


def run_module():
    """Read from params.json and generate updated claims-based hospitalization indicator values."""
    start_time = time.time()
    params = read_params()
    logger = get_structured_logger(
        __name__, filename=params.get("log_filename"),
        log_exceptions=params.get("log_exceptions", True))

    # handle range of estimates to produce
    # filename expected to have format: EDI_AGG_INPATIENT_DDMMYYYY_HHMM{timezone}.csv.gz
    if params["drop_date"] is None:
        dropdate_dt = datetime.strptime(
            Path(params["input_file"]).name.split("_")[3], "%d%m%Y")
    else:
        dropdate_dt = datetime.strptime(params["drop_date"], "%Y-%m-%d")

    # produce estimates for n_backfill_days
    # most recent n_waiting_days won't be est
    enddate_dt = dropdate_dt - timedelta(days=params["n_waiting_days"])
    startdate_dt = enddate_dt - timedelta(days=params["n_backfill_days"])
    enddate = str(enddate_dt.date())
    startdate = str(startdate_dt.date())
    dropdate = str(dropdate_dt.date())

    # now allow manual overrides
    if params["end_date"] is not None:
        enddate = params["end_date"]
    if params["start_date"] is not None:
        startdate = params['start_date']

    # print out information
    logger.info("Loaded params",
                startdate = startdate,
                enddate = enddate,
                dropdate = dropdate,
                n_backfill_days = params["n_backfill_days"],
                n_waiting_days = params["n_waiting_days"],
                geos = params["geos"],
                outpath = params["export_dir"],
                parallel = params["parallel"],
                weekday = params["weekday"],
                write_se = params["write_se"])

    # generate indicator csvs
    for geo in params["geos"]:
        for weekday in params["weekday"]:
            if weekday:
                logger.info("starting weekday adj", geo = geo)
            else:
                logger.info("starting no weekday adj", geo =  geo)

            signal_name = Config.signal_weekday_name if weekday else Config.signal_name
            if params["write_se"]:
                assert params["obfuscated_prefix"] is not None, \
                    "supply obfuscated prefix in params.json"
                signal_name = params["obfuscated_prefix"] + "_" + signal_name

            logger.info("Updating signal name", signal_name = signal_name)
            updater = ClaimsHospIndicatorUpdater(
                startdate,
                enddate,
                dropdate,
                geo,
                params["parallel"],
                weekday,
                params["write_se"],
                signal_name
            )
            updater.update_indicator(params["input_file"], params["export_dir"])
        logger.info("finished updating", geo = geo)
    
    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    logger.info("Completed indicator run",
        elapsed_time_in_seconds = elapsed_time_in_seconds)
