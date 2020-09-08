# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_validator`.
"""
from datetime import datetime
import numpy as np
import pandas as pd
from delphi_utils import read_params
from .validate import validate
from .datafetcher import read_filenames


def run_module():
    parent_params = read_params()
    params = parent_params['validation']
    dtobj_sdate = datetime.strptime(params['start_date'], '%Y-%m-%d')
    dtobj_edate = datetime.strptime(params['end_date'], '%Y-%m-%d')
    max_check_lookbehind = int(params["ref_window_size"])
    validate(parent_params["export_dir"], dtobj_sdate, dtobj_edate)
