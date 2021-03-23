# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_changehc`.
"""

# standard packages
import time
from datetime import datetime, timedelta
from typing import Dict, Any

#  third party
from delphi_utils import get_structured_logger

# first party
from .download_ftp_files import download_covid, download_cli
from .load_data import load_combined_data, load_cli_data
from .update_sensor import CHCSensorUpdator


def retrieve_files(params, filedate, logger):
    """Return filenames of relevant files, downloading them if necessary."""
    files = params["indicator"]["input_files"]
    if files["denom"] is None:

        ## download recent files from FTP server
        logger.info("downloading recent files through SFTP")
        if "covid" in params["indicator"]["types"]:
            download_covid(filedate, params["indicator"]["input_cache_dir"], params["indicator"]["ftp_conn"])
        if "cli" in params["indicator"]["types"]:
            download_cli(filedate, params["indicator"]["input_cache_dir"], params["indicator"]["ftp_conn"])

        denom_file = "%s/%s_Counts_Products_Denom.dat.gz" % (params["indicator"]["input_cache_dir"],filedate)
        covid_file = "%s/%s_Counts_Products_Covid.dat.gz" % (params["indicator"]["input_cache_dir"],filedate)
        flu_file = "%s/%s_Counts_Products_Flu.dat.gz" % (params["indicator"]["input_cache_dir"],filedate)
        mixed_file = "%s/%s_Counts_Products_Mixed.dat.gz" % (params["indicator"]["input_cache_dir"],filedate)
        flu_like_file = "%s/%s_Counts_Products_Flu_Like.dat.gz" % (params["indicator"]["input_cache_dir"],filedate)
        covid_like_file = "%s/%s_Counts_Products_Covid_Like.dat.gz" % (params["indicator"]["input_cache_dir"],filedate)
    else:
        denom_file = files["denom"]
        covid_file = files["covid"]
        flu_file = files["flu"]
        mixed_file = files["mixed"]
        flu_like_file = files["flu_like"]
        covid_like_file = files["covid_like"]

    file_dict = {"denom": denom_file}
    if "covid" in params["indicator"]["types"]:
        file_dict["covid"] = covid_file
    if "cli" in params["indicator"]["types"]:
        file_dict["flu"] = flu_file
        file_dict["mixed"] = mixed_file
        file_dict["flu_like"] = flu_like_file
        file_dict["covid_like"] = covid_like_file
    return file_dict


def make_asserts(params):
    """Assert that for each type, filenames are either all present or all absent."""
    files = params["indicator"]["input_files"]
    if "covid" in params["indicator"]["types"]:
        assert (files["denom"] is None) == (files["covid"] is None), \
            "exactly one of denom and covid files are provided"
    if "cli" in params["indicator"]["types"]:
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


def run_module(params: Dict[str, Dict[str, Any]]):
    """
    Run the delphi_changehc module.

    Parameters
    ----------
    params
        Dictionary containing indicator configuration. Expected to have the following structure:
        - "common":
            - "export_dir": str, directory to write output.
            - "log_exceptions" (optional): bool, whether to log exceptions to file.
            - "log_filename" (optional): str, name of file to write logs
        - "indicator":
            - "input_cache_dir": str, directory to download source files.
            - "input_files": dict of str: str or null, optional filenames to download. If null,
                defaults are set in retrieve_files().
            - "start_date": str, YYYY-MM-DD format, first day to generate data for.
            - "end_date": str or null, YYYY-MM-DD format, last day to generate data for.
               If set to null, end date is derived from drop date and n_waiting_days.
            - "drop_date": str or null, YYYY-MM-DD format, date data is dropped. If set to
               null, current day minus 40 hours is used.
            - "n_backfill_days": int, number of past days to generate estimates for.
            - "n_waiting_days": int, number of most recent days to skip estimates for.
            - "se": bool, whether to write out standard errors.
            - "parallel": bool, whether to update sensor in parallel.
            - "geos": list of str, geographies to generate sensor for.
            - "weekday": list of bool, whether to adjust for weekday effects.
            - "types": list of str, sensor types to generate.
            - "wip_signal": list of str or bool, to be passed to delphi_utils.add_prefix.
            - "ftp_conn": dict, connection information for source FTP.
    """
    start_time = time.time()

    logger = get_structured_logger(
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))

    make_asserts(params)

    if params["indicator"]["drop_date"] is None:
        # files are dropped about 4pm the day after the issue date
        dropdate_dt = (datetime.now() - timedelta(days=1,hours=16))
        dropdate_dt = dropdate_dt.replace(hour=0,minute=0,second=0,microsecond=0)
    else:
        dropdate_dt = datetime.strptime(params["indicator"]["drop_date"], "%Y-%m-%d")
    filedate = dropdate_dt.strftime("%Y%m%d")

    file_dict = retrieve_files(params, filedate, logger)

    dropdate = str(dropdate_dt.date())

    # range of estimates to produce
    n_backfill_days = params["indicator"]["n_backfill_days"]  # produce estimates for n_backfill_days
    n_waiting_days = params["indicator"]["n_waiting_days"]  # most recent n_waiting_days won't be est
    enddate_dt = dropdate_dt - timedelta(days=n_waiting_days)
    startdate_dt = enddate_dt - timedelta(days=n_backfill_days)
    enddate = str(enddate_dt.date())
    startdate = str(startdate_dt.date())

    # now allow manual overrides
    if params["indicator"]["end_date"] is not None:
        enddate = params["indicator"]["end_date"]
    if params["indicator"]["start_date"] is not None:
        startdate = params["indicator"]["start_date"]

    logger.info("generating signal and exporting to CSV",
        first_sensor_date = startdate,
        last_sensor_date = enddate,
        drop_date = dropdate,
        n_backfill_days = n_backfill_days,
        n_waiting_days = n_waiting_days,
        geos = params["indicator"]["geos"],
        export_dir = params["common"]["export_dir"],
        parallel = params["indicator"]["parallel"],
        weekday = params["indicator"]["weekday"],
        types = params["indicator"]["types"],
        se = params["indicator"]["se"])

    ## start generating
    for geo in params["indicator"]["geos"]:
        for numtype in params["indicator"]["types"]:
            for weekday in params["indicator"]["weekday"]:
                if weekday:
                    logger.info("starting weekday adj", geo = geo, numtype = numtype)
                else:
                    logger.info("starting no adj", geo = geo, numtype = numtype)
                su_inst = CHCSensorUpdator(
                    startdate,
                    enddate,
                    dropdate,
                    geo,
                    params["indicator"]["parallel"],
                    weekday,
                    numtype,
                    params["indicator"]["se"],
                    params["indicator"]["wip_signal"]
                )
                if numtype == "covid":
                    data = load_combined_data(file_dict["denom"],
                             file_dict["covid"],dropdate_dt,"fips")
                elif numtype == "cli":
                    data = load_cli_data(file_dict["denom"],file_dict["flu"],file_dict["mixed"],
                             file_dict["flu_like"],file_dict["covid_like"],dropdate_dt,"fips")
                su_inst.update_sensor(
                    data,
                    params["common"]["export_dir"]
                )
            logger.info("finished processing", geo = geo)

    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    logger.info("Completed indicator run",
        elapsed_time_in_seconds = elapsed_time_in_seconds)
