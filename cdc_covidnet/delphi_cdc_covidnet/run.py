# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_cdc_covidnet`.
"""
import logging
from datetime import datetime
from os import remove
from os.path import join

from delphi_utils import read_params

from .covidnet import CovidNet
from .update_sensor import update_sensor


def run_module():
    """Parse parameters and generates csv files for the COVID-NET sensor."""
    params = read_params()

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
        end_date)

    # Cleanup cache dir
    remove(mappings_file)
    for state_file in state_files:
        remove(state_file)

    logging.info("finished all")
