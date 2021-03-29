# -*- coding: utf-8 -*-
"""Functions to call when running the tool.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_validator`.
"""
import argparse as ap
from .. import read_params
from .validate import Validator


def run_module():
    """Run the validator as a module."""
    parser = ap.ArgumentParser()
    parser.add_argument("--dry_run", action="store_true", help="When provided, return zero exit"
                        " status irrespective of the number of failures")
    args = parser.parse_args()
    validator = Validator(read_params())
    validator.validate().print_and_exit(not args.dry_run)


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
