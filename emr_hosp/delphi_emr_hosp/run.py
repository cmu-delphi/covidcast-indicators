# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_emr_hosp`.
"""

# standard packages
import logging
from datetime import datetime
from pathlib import Path

#  third party
from delphi_utils import read_params

# first party
from .update_sensor import update_sensor


def run_module():
    params = read_params()

    logging.basicConfig(level=logging.DEBUG)
    logging.info("start date:\t%s", params["start_date"])

    ## get end date from input file
    # the filenames are expected to be in the format:
    # EMR: "ICUE_CMB_INPATIENT_DDMMYYYY.csv.gz"
    # CLAIMS: "EDI_AGG_INPATIENT_DDMMYYYY_HHMM{timezone}.csv.gz"
    if params["drop_date"] == "":
        dropdate_emr = str(
            datetime.strptime(
                Path(params["input_emr_file"]).name.split("_")[3].split(".")[0], "%d%m%Y"
            ).date()
        )
        dropdate_claims = str(
            datetime.strptime(
                Path(params["input_claims_file"]).name.split("_")[3], "%d%m%Y"
            ).date()
        )

        assert dropdate_emr == dropdate_claims, "different drop dates for data steams"
        dropdate = dropdate_claims
    else:
        dropdate = params["drop_date"]

    logging.info("drop date:\t%s", dropdate)

    ## geographies
    geos = ["state", "msa", "hrr", "county"]

    ## print out other vars
    logging.info("outpath:\t%s", params["export_dir"])
    logging.info("parallel:\t%s", params["parallel"])
    logging.info("weekday:\t%s", params["weekday"])

    ## start generating
    for geo in geos:
        for weekday in params["weekday"]:
            if weekday:
                logging.info("starting %s, weekday adj", geo)
            else:
                logging.info("starting %s, no adj", geo)
            update_sensor(
                params["input_emr_file"],
                params["input_claims_file"],
                params["export_dir"],
                params["static_file_dir"],
                params["start_date"],
                params["end_date"],
                dropdate,
                geo,
                params["parallel"],
                weekday,
            )
        logging.info("finished %s", geo)

    logging.info("finished all")
