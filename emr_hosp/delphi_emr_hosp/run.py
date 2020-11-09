# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_emr_hosp`.
"""

# standard packages
import logging
from datetime import datetime, timedelta
from pathlib import Path

#  third party
from delphi_utils import read_params

# first party
from .update_sensor import EMRHospSensorUpdator


def run_module():
    """Run the delphi_emr_hosp module.
    """

    params = read_params()

    logging.basicConfig(level=logging.DEBUG)

    ## get end date from input file
    # the filenames are expected to be in the format:
    # EMR: "ICUE_CMB_INPATIENT_DDMMYYYY.csv.gz"
    # CLAIMS: "EDI_AGG_INPATIENT_DDMMYYYY_HHMM{timezone}.csv.gz"
    if params["drop_date"] is None:
        dropdate_emr = datetime.strptime(
            Path(params["input_emr_file"]).name.split("_")[3].split(".")[0], "%d%m%Y"
        )

        dropdate_claims = datetime.strptime(
            Path(params["input_claims_file"]).name.split("_")[3], "%d%m%Y"
        )
        assert dropdate_emr == dropdate_claims, "different drop dates for data steams"
        dropdate_dt = dropdate_claims
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
            su_inst = EMRHospSensorUpdator(
                startdate,
                enddate,
                dropdate,
                geo,
                params["parallel"],
                weekday,
                params["se"]
            )
            su_inst.update_sensor(
                params["input_emr_file"],
                params["input_claims_file"],
                params["export_dir"],
                params["static_file_dir"]
            )
        logging.info("finished %s", geo)

    logging.info("finished all")
