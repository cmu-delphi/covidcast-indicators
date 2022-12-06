# -*- coding: utf-8 -*-
"""Functions to call when running the tool.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_utils.flash_eval`.
"""

from .. import read_params
from .eval_day import flash_eval


def run_module():
    """Run the validator as a module."""
    params = read_params()
    flash_eval(params)



def flagger_from_params(params, df=None):
    """Construct a validator from `params`.

    Arguments
    ---------
    params: Dict[str, Any]
        Dictionary of parameters
    """
    return flash_eval(params, df)
