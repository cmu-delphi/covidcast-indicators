# -*- coding: utf-8 -*-
"""Functions to call when running the tool.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_validator`.
"""
from delphi_utils import read_params
from .validate import Validator


def run_module():
    """Run the validator as a module."""
    parent_params = read_params()
    params = parent_params['validation']

    validator = Validator(params)
    validator.validate(parent_params["export_dir"]).print_and_exit()
