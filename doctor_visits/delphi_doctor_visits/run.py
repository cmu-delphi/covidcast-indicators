# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_doctor_visits`.
"""

# standard packages
import logging
from datetime import datetime
from pathlib import Path

#  third party
from delphi_utils import read_params

# first party
from .update_sensor import update_sensor


def run_module():

    params = read_params()

    logging.basicConfig(level=logging.DEBUG)

    ## start date will be Jan 1
    logging.info("start date:\t%s", params["start_date"])

    ## get end date from input file
    # the filename is expected to be in the format:
    # "EDI_AGG_OUTPATIENT_DDMMYYYY_HHMM{timezone}.csv.gz"
    if params["end_date"] == "":
        dropdate = str(
            datetime.strptime(
                Path(params["input_file"]).name.split("_")[3], "%d%m%Y"
            ).date()
        )
    else:
        dropdate = params["end_date"]

    logging.info("drop date:\t%s", dropdate)

    ## geographies
    geos = ["state", "msa", "hrr", "county"]

    ## print out other vars
    logging.info("outpath:\t%s", params["export_dir"])
    logging.info("parallel:\t%s", params["parallel"])

    ## start generating
    for geo in geos:
        for weekday in [True, False]:
            if weekday:
                logging.info("starting %s, weekday adj", geo)
            else:
                logging.info("starting %s, no adj", geo)
            update_sensor(
                params["input_file"],
                params["export_dir"],
                params["static_file_dir"],
                params["start_date"],
                dropdate,
                geo,
                params["parallel"],
                weekday,
            )
        logging.info("finished %s", geo)

    logging.info("finished all")
