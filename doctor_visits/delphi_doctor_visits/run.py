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


def run_module(params):
    """
    Run doctor visits indicator.

    Parameters
    ----------
    params
        Dictionary containing indicator configuration. Expected to have the following structure:
        - "common":
            - "export_dir": str, directory to write output.
        - "indicator":
            - "input_file": str, path to aggregated doctor-visits data.
            - "drop_date": str, YYYY-MM-DD format, date data is dropped. If set to
               empty string, current day minus 40 hours is used.
            - "n_backfill_days": int, number of past days to generate estimates for.
            - "n_waiting_days": int, number of most recent days to skip estimates for.
            - "weekday": list of bool, which weekday adjustments to perform. For each value in the list, signals will
                be generated with weekday adjustments (True) or without adjustments (False)
            - "se": bool, whether to write out standard errors
            - "obfuscated_prefix": str, prefix for signal name if write_se is True.
            - "parallel": bool, whether to update sensor in parallel.
    """
    logging.basicConfig(level=logging.DEBUG)

    ## get end date from input file
    # the filename is expected to be in the format:
    # "EDI_AGG_OUTPATIENT_DDMMYYYY_HHMM{timezone}.csv.gz"
    if params["indicator"]["drop_date"] == "":
        dropdate_dt = datetime.strptime(
            Path(params["indicator"]["input_file"]).name.split("_")[3], "%d%m%Y"
        )
    else:
        dropdate_dt = datetime.strptime(params["indicator"]["drop_date"], "%Y-%m-%d")
    dropdate = str(dropdate_dt.date())

    # range of estimates to produce
    n_backfill_days = params["indicator"]["n_backfill_days"] # produce estimates for n_backfill_days
    n_waiting_days = params["indicator"]["n_waiting_days"]  # most recent n_waiting_days won't be est
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
    logging.info("outpath:\t\t%s", params["common"]["export_dir"])
    logging.info("parallel:\t\t%s", params["indicator"]["parallel"])
    logging.info(f"weekday:\t\t%s", params["indicator"]["weekday"])
    logging.info(f"write se:\t\t%s", params["indicator"]["se"])
    logging.info(f"obfuscated prefix:\t%s", params["indicator"]["obfuscated_prefix"])

    ## start generating
    for geo in geos:
        for weekday in params["indicator"]["weekday"]:
            if weekday:
                logging.info("starting %s, weekday adj", geo)
            else:
                logging.info("starting %s, no adj", geo)
            update_sensor(
                filepath=params["indicator"]["input_file"],
                outpath=params["common"]["export_dir"],
                startdate=startdate,
                enddate=enddate,
                dropdate=dropdate,
                geo=geo,
                parallel=params["indicator"]["parallel"],
                weekday=weekday,
                se=params["indicator"]["se"],
                prefix=params["indicator"]["obfuscated_prefix"]
            )
        logging.info("finished %s", geo)

    logging.info("finished all")
