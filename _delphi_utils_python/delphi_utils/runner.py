"""Indicator running utilities."""
from typing import Any, Callable, Dict, Optional
from delphi_validator.validate import Validator
from .archive import ArchiveDiffer
from .utils import read_params

def run_indicator_pipeline(indicator_fn:  Callable[[Dict[str, Any]], None],
                           validator_fn:  Callable[[Dict[str, Any]], Optional[Validator]],
                           archiver_fn:  Callable[[Dict[str, Any]], Optional[ArchiveDiffer]]):
    """Runs the indicator."""
    params = read_params()
    indicator_fn(params)
    validator = validator_fn(params)
    archiver = archiver_fn(params)
    if validator:
        validation_report = validator.validate()
    if archiver and (not validator or validation_report.success()):
        archiver.archive()
