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
from .download_ftp_files import download_covid, download_cli
from .load_data import load_combined_data, load_cli_data
from .update_sensor import CHCSensorUpdator


def run_module(): # pylint: disable=too-many-branches,too-many-statements
    """Run the delphi_changehc module.
    """

    params = read_params()

    logging.basicConfig(level=logging.DEBUG)

    # the filenames are expected to be in the format:
    # Denominator: "YYYYMMDD_All_Outpatients_By_County.dat.gz"
    # Numerator: "YYYYMMDD_Covid_Outpatients_By_County.dat.gz"

    assert (params["input_denom_file"] is None) == (params["input_covid_file"] is None), \
        "exactly one of denom and covid files are provided"

    if params["drop_date"] is None:
        # files are dropped about 8pm the day after the issue date
        dropdate_dt = (datetime.now() - timedelta(days=1,hours=20))
        dropdate_dt = dropdate_dt.replace(hour=0,minute=0,second=0,microsecond=0)
    else:
        dropdate_dt = datetime.strptime(params["drop_date"], "%Y-%m-%d")
    filedate = dropdate_dt.strftime("%Y%m%d")

    if params["input_denom_file"] is None:

        ## download recent files from FTP server
        logging.info("downloading recent files through SFTP")
        if "covid" in params["types"]:
            download_covid(params["cache_dir"], params["ftp_conn"])
        if "cli" in params["types"]:
            download_cli(params["cache_dir"], params["ftp_conn"])

        input_denom_file = "%s/%s_All_Outpatients_By_County.dat.gz" % (params["cache_dir"],filedate)
        input_covid_file = "%s/%s_Covid_Outpatients_By_County.dat.gz" % (params["cache_dir"],filedate)
        input_flu_file = "%s/%s_Flu_Patient_Count_By_County.dat.gz" % (params["cache_dir"],filedate)
        input_mixed_file = "%s/%s_Mixed_Patient_Count_By_County.dat.gz" % (params["cache_dir"],filedate)
        input_flu_like_file = "%s/%s_Flu_Like_Patient_Count_By_County.dat.gz" % (params["cache_dir"],filedate)
        input_covid_like_file = "%s/%s_Covid_Like_Patient_Count_By_County.dat.gz" % (params["cache_dir"],filedate)
    else:
        input_denom_file = params["input_denom_file"]
        input_covid_file = params["input_covid_file"]
        input_flu_file = params["input_flu_file"]
        input_mixed_file = params["input_mixed_file"]
        input_flu_like_file = params["input_flu_like_file"]
        input_covid_like_file = params["input_covid_like_file"]

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
        startdate = params["start_date"]

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
    logging.info("types:\t\t%s", params["types"])
    logging.info("se:\t\t\t%s", params["se"])

    ## start generating
    for geo in params["geos"]:
        for numtype in params["types"]:
            for weekday in params["weekday"]:
                if weekday:
                    logging.info("starting %s, %s, weekday adj", geo, numtype)
                else:
                    logging.info("starting %s, %s, no adj", geo, numtype)
                su_inst = CHCSensorUpdator(
                    startdate,
                    enddate,
                    dropdate,
                    geo,
                    params["parallel"],
                    weekday,
                    numtype,
                    params["se"]
                )
                if numtype == "covid":
                    data = load_combined_data(input_denom_file,
                             input_covid_file,dropdate_dt,"fips")
                elif numtype == "cli":
                    data = load_cli_data(input_denom_file,input_flu_file,input_mixed_file,
                             input_flu_like_file,input_covid_like_file,dropdate_dt,"fips")
                su_inst.update_sensor(
                    data,
                    params["export_dir"]
                )
            logging.info("finished %s", geo)

    logging.info("finished all")
