"""Indicator running utilities."""
from typing import Any, Callable, Dict, Optional
from .archive import ArchiveDiffer
from .utils import read_params
from .validator.validate import Validator

Params = Dict[str, Any]

# Trivial function to use as default value for validator and archive functions.
NULL_FN = lambda x: None

def run_indicator_pipeline(indicator_fn:  Callable[[Params], None],
                           validator_fn:  Callable[[Params], Optional[Validator]] = NULL_FN,
                           archiver_fn:  Callable[[Params], Optional[ArchiveDiffer]] = NULL_FN):
    """Run an indicator with its optional validation and archiving.

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
    if archiver and (not validator or validation_report.success()):
        archiver.archive()
