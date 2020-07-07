# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module` that is executed
when the module is run with `python -m MODULE_NAME`.
"""

import datetime
import logging

from delphi_utils import read_params

from .pull_api import GoogleHealthTrends, get_counts_states, get_counts_dma
from .map_values import derived_counts_from_dma
from .export import export_csv

def run_module():
    """Main function run when calling the module.

    Inputs parameters from the file 'params.json' and produces output data in
    the directory defined by the `export_dir` (should be "receiving" expect for
    testing purposes).
    """

    #  read parameters
    params = read_params()
    ght_key = params["ght_key"]
    start_date = params["start_date"]
    end_date = params["end_date"]
    static_dir = params["static_file_dir"]
    export_dir = params["export_dir"]
    cache_dir = params["cache_dir"]

    # if missing end_date, set to today (GMT) minus 5 days
    if end_date == "":
        now = datetime.datetime.now(datetime.timezone.utc)
        end_date = (now - datetime.timedelta(days=4)).strftime("%Y-%m-%d")

    # Turn on basic logging messages (level INFO)
    logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)
    logging.info("Creating data from %s through %s.", start_date, end_date)

    # setup class to handle API calls
    ght = GoogleHealthTrends(ght_key=ght_key)

    #  read data frame version of the data
    df_state = get_counts_states(
        ght, start_date, end_date, static_dir=static_dir, cache_dir=cache_dir
    )
    df_dma = get_counts_dma(
        ght, start_date, end_date, static_dir=static_dir, cache_dir=cache_dir
    )
    df_hrr, df_msa = derived_counts_from_dma(df_dma, static_dir=static_dir)

    #  export each geographic region, with both smoothed and unsmoothed data
    
    export_csv(df_state, "state", "raw_search", smooth="raw", receiving_dir=export_dir)
    export_csv(df_state, "state", "smoothed_search", smooth="smooth", receiving_dir=export_dir)
    export_csv(df_state, "state", "wip_smoothed_search", smooth="wip", receiving_dir=export_dir)

    export_csv(df_dma, "dma", "raw_search", smooth="raw", receiving_dir=export_dir)
    export_csv(df_dma, "dma", "smoothed_search", smooth="smooth", receiving_dir=export_dir)
    export_csv(df_dma, "dma", "wip_smoothed_search", smooth="wip", receiving_dir=export_dir)

    export_csv(df_hrr, "hrr", "raw_search", smooth="raw", receiving_dir=export_dir)
    export_csv(df_hrr, "hrr", "smoothed_search", smooth="smooth", receiving_dir=export_dir)
    export_csv(df_hrr, "hrr", "wip_smoothed_search", smooth="wip", receiving_dir=export_dir)

    export_csv(df_msa, "msa", "raw_search", smooth="raw", receiving_dir=export_dir)
    export_csv(df_msa, "msa", "smoothed_search", smooth="smooth", receiving_dir=export_dir)
    export_csv(df_msa, "msa", "wip_smoothed_search", smooth="wip", receiving_dir=export_dir)
