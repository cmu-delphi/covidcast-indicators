# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_claims_hosp`.
"""

# standard packages
import logging
from datetime import datetime, timedelta
from pathlib import Path

# third party
from delphi_utils import read_params

# first party
from .config import Config
from .update_indicator import ClaimsHospIndicatorUpdater


def run_module():
    """Read from params.json and generate updated claims-based hospitalization indicator values."""
    params = read_params()
    logging.basicConfig(level=logging.DEBUG)

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
    logging.info("first sensor date:\t%s", startdate)
    logging.info("last sensor date:\t%s", enddate)
    logging.info("drop date:\t\t%s", dropdate)
    logging.info("n_backfill_days:\t%s", params["n_backfill_days"])
    logging.info("n_waiting_days:\t%s", params["n_waiting_days"])
    logging.info("geos:\t\t\t%s", params["geos"])
    logging.info("outpath:\t\t%s", params["export_dir"])
    logging.info("parallel:\t\t%s", params["parallel"])
    logging.info("weekday:\t\t%s", params["weekday"])
    logging.info("write_se:\t\t%s", params["write_se"])

    # generate indicator csvs
    for geo in params["geos"]:
        for weekday in params["weekday"]:
            if weekday:
                logging.info("starting %s, weekday adj", geo)
            else:
                logging.info("starting %s, no adj", geo)

            signal_name = Config.signal_weekday_name if weekday else Config.signal_name
            if params["write_se"]:
                assert params["obfuscated_prefix"] is not None, \
                    "supply obfuscated prefix in params.json"
                signal_name = params["obfuscated_prefix"] + "_" + signal_name

            logging.info("output signal name %s", signal_name)
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
        logging.info("finished %s", geo)
    logging.info("finished all")
