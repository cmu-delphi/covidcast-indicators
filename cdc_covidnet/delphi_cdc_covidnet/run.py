# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_cdc_covidnet`.
"""
import logging
from datetime import datetime
from os import remove
from os.path import join
from typing import Dict, Any

from .covidnet import CovidNet
from .update_sensor import update_sensor


def run_module(params: Dict[str, Dict[str, Any]]):
    """
    Parse parameters and generates csv files for the COVID-NET sensor.

    Parameters
    ----------
    params
        Dictionary containing indicator configuration. Expected to have the following structure:
        - "common":
            - "export_dir": str, directory to write output.
        - "indicator":
            - "start_date": str, YYYY-MM-DD format, first day to generate data for.
            - "end_date": str, YYYY-MM-DD format or empty string, last day to generate data for.
                If set to empty string, current day will be used.
            - "parallel": bool, whether to download source files in parallel.
            - "wip_signal": list of str or bool, to be passed to delphi_utils.add_prefix.
            - "input_cache_dir": str, directory to download source files.
    """
    logging.basicConfig(level=logging.DEBUG)

    start_date = datetime.strptime(params["indicator"]["start_date"], "%Y-%m-%d")

    # If no end_date is specified, assume it is the current date
    if params["indicator"]["end_date"] == "":
        end_date = datetime.now()
    else:
        end_date = datetime.strptime(params["indicator"]["end_date"], "%Y-%m-%d")

    logging.info("start date:\t%s", start_date.date())
    logging.info("end date:\t%s", end_date.date())

    logging.info("outpath:\t%s", params["common"]["export_dir"])
    logging.info("parallel:\t%s", params["indicator"]["parallel"])

    # Only geo is state, and no weekday adjustment for now
    # COVID-NET data is by weeks anyway, not daily
    logging.info("starting state, no adj")

    # Download latest COVID-NET files into the cache directory first
    mappings_file = join(params["indicator"]["input_cache_dir"], "init.json")
    CovidNet.download_mappings(outfile=mappings_file)
    _, mmwr_info, _ = CovidNet.read_mappings(mappings_file)
    state_files = CovidNet.download_all_hosp_data(
        mappings_file, params["indicator"]["input_cache_dir"],
        parallel=params["indicator"]["parallel"])

    update_sensor(
        state_files,
        mmwr_info,
        params["common"]["export_dir"],
        start_date,
        end_date,
        params["indicator"]["wip_signal"])

    # Cleanup cache dir
    remove(mappings_file)
    for state_file in state_files:
        remove(state_file)

    logging.info("finished all")
