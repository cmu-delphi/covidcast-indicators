# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.  Its lone argument should be a nested
dictionary of parameters loaded from the params.json file.  We expect the `params` to have the
following structure:
    - "common":
        - "export_dir": str, directory to which the results are exported
        - "log_filename": (optional) str, path to log file
    - "indicator": (optional)
        - "wip_signal": (optional) Any[str, bool], list of signals that are works in progress, or
            True if all signals in the registry are works in progress, or False if only
            unpublished signals are.  See `delphi_utils.add_prefix()`
        - Any other indicator-specific settings
"""
from delphi_utils import add_prefix
from .constants import SIGNALS

def run_module(params):
    """
    Runs the indicator

    Arguments
    --------
    params:  Dict[str, Any]
        Nested dictionary of parameters.

    Returns
    -------
    prints the updated signal names
    """
    wip_signal = params["indicator"]["wip_signal"]
    signal_names = add_prefix(SIGNALS, wip_signal, prefix="wip_")
    print(signal_names)
