# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.
"""
import numpy as np
import pandas as pd
from delphi_utils import read_params


def run_module():

    params = read_params()
