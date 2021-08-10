# -*- coding: utf-8 -*-
"""Functions to call when running the tool.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_utils.validator`.
"""
import argparse as ap
from .. import read_params, get_structured_logger
from .validate import Validator


def run_module():
    """Run the validator as a module."""
    parser = ap.ArgumentParser()
    parser.add_argument("--dry_run", action="store_true", help="When provided, return zero exit"
                        " status irrespective of the number of failures")
    args = parser.parse_args()
    params = read_params()
    assert "validation" in params
    dry_run_param = params["validation"]["common"].get("dry_run", False)
    params["validation"]["common"]["dry_run"] = args.dry_run or dry_run_param
    validator = Validator(params)
    validator.validate().print_and_exit(
        get_structured_logger(__name__,
                              params["common"].get("log_filename", None)),
        not (args.dry_run or dry_run_param))


def validator_from_params(params):
    """Construct a validator from `params`.

    Arguments
    ---------
    params: Dict[str, Any]
        Dictionary of parameters
    """
    if "validation" in params:
        return Validator(params)
    return None
