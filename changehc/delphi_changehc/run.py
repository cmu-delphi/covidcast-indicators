# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_changehc`.
"""

# standard packages
import logging
from datetime import datetime, timedelta
from pathlib import Path

#  third party
from delphi_utils import read_params

# first party
from .update_sensor import CHCSensorUpdator


def run_module():
    """Run the delphi_changehc module.
    """

    params = read_params()

    logging.basicConfig(level=logging.DEBUG)

    ## get end date from input file
    # the filenames are expected to be in the format:
    # Denominator: "YYYYMMDD_All_Outpatients_By_County.dat.gz"
    # Numerator: "YYYYMMDD_Covid_Outpatients_By_County.dat.gz"
    if params["drop_date"] is None:
        dropdate_denom = datetime.strptime(
            Path(params["input_denom_file"]).name.split("_")[0], "%Y%m%d"
        )

        dropdate_covid = datetime.strptime(
            Path(params["input_covid_file"]).name.split("_")[0], "%Y%m%d"
        )
        assert dropdate_denom == dropdate_covid, "different drop dates for data files"
        dropdate_dt = dropdate_denom
    else:
        dropdate_dt = datetime.strptime(params["drop_date"], "%Y-%m-%d")
    dropdate = str(dropdate_dt.date())

    # range of estimates to produce
    n_backfill_days = params["n_backfill_days"]  # produce estimates for n_backfill_days
    n_waiting_days = params["n_waiting_days"]  # most recent n_waiting_days won't be est
    enddate_dt = dropdate_dt - timedelta(days=n_waiting_days)
    startdate_dt = enddate_dt - timedelta(days=n_backfill_days)
    enddate = str(enddate_dt.date())
    startdate = str(startdate_dt.date())

    # now allow manual overrides
    if params["end_date"] is not None:
        enddate = params["end_date"]
    if params["start_date"] is not None:
        startdate = params['start_date']

    logging.info("first sensor date:\t%s", startdate)
    logging.info("last sensor date:\t%s", enddate)
    logging.info("drop date:\t\t%s", dropdate)
    logging.info("n_backfill_days:\t%s", n_backfill_days)
    logging.info("n_waiting_days:\t%s", n_waiting_days)

    ## print out other vars
    logging.info("geos:\t\t\t%s", params["geos"])
    logging.info("outpath:\t\t%s", params["export_dir"])
    logging.info("parallel:\t\t%s", params["parallel"])
    logging.info("weekday:\t\t%s", params["weekday"])
    logging.info("se:\t\t\t%s", params["se"])

    ## start generating
    for geo in params["geos"]:
        for weekday in params["weekday"]:
            if weekday:
                logging.info("starting %s, weekday adj", geo)
            else:
                logging.info("starting %s, no adj", geo)
            su_inst = CHCSensorUpdator(
                startdate,
                enddate,
                dropdate,
                geo,
                params["parallel"],
                weekday,
                params["se"]
            )
            su_inst.update_sensor(
                params["input_denom_file"],
                params["input_covid_file"],
                params["export_dir"],
                params["static_file_dir"]
            )
        logging.info("finished %s", geo)

    logging.info("finished all")
