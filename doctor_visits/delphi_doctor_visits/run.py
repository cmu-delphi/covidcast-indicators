# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_doctor_visits`.
"""

# standard packages
import logging
from datetime import datetime, timedelta
from pathlib import Path

#  third party
from delphi_utils import read_params

# first party
from .update_sensor import update_sensor


def run_module():

    params = read_params()

    logging.basicConfig(level=logging.DEBUG)

    ## get end date from input file
    # the filename is expected to be in the format:
    # "EDI_AGG_OUTPATIENT_DDMMYYYY_HHMM{timezone}.csv.gz"
    if params["drop_date"] == "":
        dropdate_dt = datetime.strptime(
            Path(params["input_file"]).name.split("_")[3], "%d%m%Y"
        )
    else:
        dropdate_dt = datetime.strptime(params["end_date"], "%Y-%m-%d")
    dropdate = str(dropdate_dt.date())

    # range of estimates to produce
    n_backfill_days = params["n_backfill_days"] # produce estimates for n_backfill_days
    n_waiting_days = params["n_waiting_days"]  # most recent n_waiting_days won't be est
    enddate_dt = dropdate_dt - timedelta(days=n_waiting_days)
    startdate_dt = enddate_dt - timedelta(days=n_backfill_days)
    enddate = str(enddate_dt.date())
    startdate = str(startdate_dt.date())
    logging.info(f"drop date:\t\t{dropdate}")
    logging.info(f"first sensor date:\t{startdate}")
    logging.info(f"last sensor date:\t{enddate}")
    logging.info(f"n_backfill_days:\t{n_backfill_days}")
    logging.info(f"n_waiting_days:\t{n_waiting_days}")

    ## geographies
    geos = ["state", "msa", "hrr", "county"]

    ## print out other vars
    logging.info("outpath:\t\t%s", params["export_dir"])
    logging.info("parallel:\t\t%s", params["parallel"])
    logging.info(f"weekday:\t\t%s", params["weekday"])
    logging.info(f"write se:\t\t%s", params["se"])
    logging.info(f"obfuscated prefix:\t%s", params["obfuscated_prefix"])

    ## start generating
    for geo in geos:
        for weekday in params["weekday"]:
            if weekday:
                logging.info("starting %s, weekday adj", geo)
            else:
                logging.info("starting %s, no adj", geo)
            update_sensor(
                filepath=params["input_file"],
                outpath=params["export_dir"],
                staticpath=params["static_file_dir"],
                startdate=startdate,
                enddate=enddate,
                dropdate=dropdate,
                geo=geo,
                parallel=params["parallel"],
                weekday=weekday,
                se=params["se"],
                prefix=params["obfuscated_prefix"]
            )
        logging.info("finished %s", geo)

    logging.info("finished all")
