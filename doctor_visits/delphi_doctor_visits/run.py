# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_doctor_visits`.
"""

# standard packages
from datetime import datetime, timedelta
from pathlib import Path

from delphi_utils import get_structured_logger

# first party
from .update_sensor import update_sensor, write_to_csv


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
            - "weekday": list of bool, which weekday adjustments to perform. For each value in the
                list, signals will be generated with weekday adjustments (True) or without
                adjustments (False)
            - "se": bool, whether to write out standard errors
            - "obfuscated_prefix": str, prefix for signal name if write_se is True.
            - "parallel": bool, whether to update sensor in parallel.
    """
    logger = get_structured_logger(
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))

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

    export_dir = params["common"]["export_dir"]
    se = params["indicator"]["se"]
    prefix = params["indicator"]["obfuscated_prefix"]

    # range of estimates to produce
    n_backfill_days = params["indicator"]["n_backfill_days"] # produce estimates for n_backfill_days
    n_waiting_days = params["indicator"]["n_waiting_days"] # most recent n_waiting_days won't be est
    enddate_dt = dropdate_dt - timedelta(days=n_waiting_days)
    startdate_dt = enddate_dt - timedelta(days=n_backfill_days)
    enddate = str(enddate_dt.date())
    startdate = str(startdate_dt.date())
    logger.info("drop date:\t\t%s", dropdate)
    logger.info("first sensor date:\t%s", startdate)
    logger.info("last sensor date:\t%s", enddate)
    logger.info("n_backfill_days:\t%s", n_backfill_days)
    logger.info("n_waiting_days:\t%s", n_waiting_days)

    ## geographies
    geos = ["state", "msa", "hrr", "county", "hhs", "nation"]


    ## print out other vars
    logger.info("outpath:\t\t%s", export_dir)
    logger.info("parallel:\t\t%s", params["indicator"]["parallel"])
    logger.info("weekday:\t\t%s", params["indicator"]["weekday"])
    logger.info("write se:\t\t%s", se)
    logger.info("obfuscated prefix:\t%s", prefix)

    ## start generating
    for geo in geos:
        for weekday in params["indicator"]["weekday"]:
            if weekday:
                logger.info("starting %s, weekday adj", geo)
            else:
                logger.info("starting %s, no adj", geo)
            sensor = update_sensor(
                filepath=params["indicator"]["input_file"],
                startdate=startdate,
                enddate=enddate,
                dropdate=dropdate,
                geo=geo,
                parallel=params["indicator"]["parallel"],
                weekday=weekday,
                se=params["indicator"]["se"],
                logger=logger,
            )
            if sensor is None:
                logger.error("No sensors calculated, no output will be produced")
                continue
            # write out results
            out_name = "smoothed_adj_cli" if weekday else "smoothed_cli"
            if params["indicator"]["se"]:
                assert prefix is not None, "template has no obfuscated prefix"
                out_name = prefix + "_" + out_name

            write_to_csv(sensor, geo, se, out_name, logger, export_dir)
            logger.debug(f"wrote files to {export_dir}")
        logger.info("finished %s", geo)

    logger.info("finished all")
