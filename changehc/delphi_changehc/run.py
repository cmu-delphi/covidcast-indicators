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

def retrieve_files(params, filedate):
    """Return filenames of relevant files, downloading them if necessary."""
    files = params["input_files"]
    if files["denom"] is None:

        ## download recent files from FTP server
        logging.info("downloading recent files through SFTP")
        if "covid" in params["types"]:
            download_covid(params["cache_dir"], params["ftp_conn"])
        if "cli" in params["types"]:
            download_cli(params["cache_dir"], params["ftp_conn"])

        denom_file = "%s/%s_All_Outpatients_By_County.dat.gz" % (params["cache_dir"],filedate)
        covid_file = "%s/%s_Covid_Outpatients_By_County.dat.gz" % (params["cache_dir"],filedate)
        flu_file = "%s/%s_Flu_Patient_Count_By_County.dat.gz" % (params["cache_dir"],filedate)
        mixed_file = "%s/%s_Mixed_Patient_Count_By_County.dat.gz" % (params["cache_dir"],filedate)
        flu_like_file = "%s/%s_Flu_Like_Patient_Count_By_County.dat.gz" % (params["cache_dir"],filedate)
        covid_like_file = "%s/%s_Covid_Like_Patient_Count_By_County.dat.gz" % (params["cache_dir"],filedate)
    else:
        denom_file = files["denom"]
        covid_file = files["covid"]
        flu_file = files["flu"]
        mixed_file = files["mixed"]
        flu_like_file = files["flu_like"]
        covid_like_file = files["covid_like"]

    file_dict = {"denom": denom_file}
    if "covid" in params["types"]:
        file_dict["covid"] = covid_file
    if "cli" in params["types"]:
        file_dict["flu"] = flu_file
        file_dict["mixed"] = mixed_file
        file_dict["flu_like"] = flu_like_file
        file_dict["covid_like"] = covid_like_file
    return file_dict


def make_asserts(params):
    """Assert that for each type, filenames are either all present or all absent."""
    files = params["input_files"]
    if "covid" in params["types"]:
        assert (files["denom"] is None) == (files["covid"] is None), \
            "exactly one of denom and covid files are provided"
    if "cli" in params["types"]:
        if files["denom"] is None:
            assert files["flu"] is None and \
                    files["mixed"] is None and \
                    files["flu_like"] is None and \
                    files["covid_like"] is None,\
                    "files must be all present or all absent"
        else:
            assert files["flu"] is not None and \
                    files["mixed"] is not None and \
                    files["flu_like"] is not None and \
                    files["covid_like"] is not None,\
                    "files must be all present or all absent"


def run_module():
    """Run the delphi_changehc module."""
    params = read_params()

    logging.basicConfig(level=logging.DEBUG)

    make_asserts(params)

    if params["drop_date"] is None:
        # files are dropped about 4pm the day after the issue date
        dropdate_dt = (datetime.now() - timedelta(days=1,hours=16))
        dropdate_dt = dropdate_dt.replace(hour=0,minute=0,second=0,microsecond=0)
    else:
        dropdate_dt = datetime.strptime(params["drop_date"], "%Y-%m-%d")
    filedate = dropdate_dt.strftime("%Y%m%d")

    file_dict = retrieve_files(params, filedate)

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
                    data = load_combined_data(file_dict["denom"],
                             file_dict["covid"],dropdate_dt,"fips")
                elif numtype == "cli":
                    data = load_cli_data(file_dict["denom"],file_dict["flu"],file_dict["mixed"],
                             file_dict["flu_like"],file_dict["covid_like"],dropdate_dt,"fips")
                su_inst.update_sensor(
                    data,
                    params["export_dir"]
                )
            logging.info("finished %s", geo)

    logging.info("finished all")
