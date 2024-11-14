# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.  `run_module`'s lone argument should be a
nested dictionary of parameters loaded from the params.json file.  We expect the `params` to have
the following structure:
    - "common":
        - "export_dir": str, directory to which the results are exported
        - "log_filename": (optional) str, path to log file
    - "indicator": (optional)
        - "wip_signal": (optional) Any[str, bool], list of signals that are works in progress, or
            True if all signals in the registry are works in progress, or False if only
            unpublished signals are.  See `delphi_utils.add_prefix()`
        - Any other indicator-specific settings
"""
import time
from datetime import timedelta, datetime
from itertools import product

import pandas as pd
from delphi_utils import get_structured_logger
from delphi_utils.export import create_export_csv
from delphi_utils.geomap import GeoMapper

from .constants import GEOS, SMOOTHERS, SIGNALS_MAP
from .pull import pull_nhsn_data


def run_module(params):
    """
    Runs the indicator

    Arguments
    --------
    params:  Dict[str, Any]
        Nested dictionary of parameters.
    """
    start_time = time.time()
    logger = get_structured_logger(
        __name__,
        filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True),
    )
    export_dir = params["common"]["export_dir"]
    backup_dir = params["common"]["backup_dir"]
    custom_run = params["common"].get("custom_run", False)
    socrata_token = params["indicator"]["socrata_token"]
    geo_mapper = GeoMapper()

    df_pull = pull_nhsn_data(socrata_token, backup_dir, custom_run=custom_run, logger=logger)

