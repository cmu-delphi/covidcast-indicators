# -*- coding: utf-8 -*-
"""Functions to call when running the tool.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_validator`.
"""
from .. import read_params
from .validate import Validator


def run_module():
    """Run the validator as a module."""
    validator = Validator(read_params())
    validator.validate().print_and_exit()


def from_params(params):
    """Construct a validator from `params`.

    Arguments
    ---------
    params: Dict[str, Any]
        Dictionary of parameters
    """
    if "validation" in params:
        return Validator(params)
    return None
