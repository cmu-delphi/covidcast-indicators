# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.
"""
from delphi_utils import read_params
from .handle_wip_signal import add_prefix

# Sample signals
SIGNALS = ["full_time_work_prop",
           "covidnet",
           "part_time_work_prop",
           "completely_home_prop"
           ]


def run_module():
    """
    Calls the method for handling the wip signals
    Returns
    -------
    prints the updated signal names
    """
    params = read_params()
    wip_signal = params["wip_signal"]
    signal_names = add_prefix(SIGNALS, wip_signal, prefix="wip_")
    print(signal_names)
