"""Indicator running utilities."""
import argparse as ap
import importlib
from typing import Any, Callable, Dict, Optional
from .archive import ArchiveDiffer, archiver_from_params
from .logger import get_structured_logger
from .utils import read_params
from .validator.validate import Validator
from .validator.run import validator_from_params

Params = Dict[str, Any]

# Trivial function to use as default value for validator and archive functions.
NULL_FN = lambda x: None

def run_indicator_pipeline(indicator_fn:  Callable[[Params], None],
                           validator_fn:  Callable[[Params], Optional[Validator]] = NULL_FN,
                           archiver_fn:  Callable[[Params], Optional[ArchiveDiffer]] = NULL_FN):
    """Run an indicator with its optional validation and archiving.

    Each argument to this function should itself be a function that will be passed a common set of
    parameters (see details below).  This parameter dictionary should have four subdictionaries
    keyed as "indicator", "validation", "archive", and "common" corresponding to parameters to be
    used in `indicator_fn`, `validator_fn`, `archiver_fn`, and shared across functions,
    respectively.

    Arguments
    ---------
    indicator_fn: Callable[[Params], None]
        function that takes a dictionary of parameters and produces indicator output
    validator_fn: Callable[[Params], Optional[Validator]]
        function that takes a dictionary of parameters and produces the associated Validator or
        None if no validation should be performed.
    archiver_fn: Callable[[Params], Optional[ArchiveDiffer]]
        function that takes a dictionary of parameters and produces the associated ArchiveDiffer or
        None if no archiving should be performed.
    """
    params = read_params()
    indicator_fn(params)
    validator = validator_fn(params)
    archiver = archiver_fn(params)
    if validator:
        validation_report = validator.validate()
        validation_report.log(get_structured_logger(
            name = indicator_fn.__module__,
            filename=params["common"].get("log_filename", None)))
    if archiver and (not validator or validation_report.success()):
        archiver.run()


if __name__ == "__main__":
    parser = ap.ArgumentParser()
    parser.add_argument("indicator_name",
                        type=str,
                        help="Name of the Python package containing the indicator.  This package "
                             "must export a `run.run_module(params)` function.")
    args = parser.parse_args()
    indicator_module = importlib.import_module(args.indicator_name)
    run_indicator_pipeline(indicator_module.run.run_module,
                           validator_from_params,
                           archiver_from_params)
