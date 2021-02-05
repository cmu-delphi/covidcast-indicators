"""Indicator running utilities."""

import abc
from logging import Logger
from typing import Any, Dict, Type

from .archive import ArchiveDiffer
from delphi_validator.validate import Validator

class Indicator(abc.ABC):
    """Abstract base class for all indicators."""

    @abc.abstractmethod
    def __init__(self, params: Dict[str, Any]):
        """Construct the indicator using key-value parameters."""
        self._name = ""
        raise NotImplementedError

    @property
    def name(self) -> str:
        """Extract the name of the indicator."""
        return self._name


    @abc.abstractmethod
    def run(self):
        """Run the indicator."""
        raise NotImplementedError


def run_indicator(indicator_type: Type[Indicator],
                  validator_type:  Type[Validator],
                  archiver_type:  Type[ArchiveDiffer],
                  params: Dict[str, Any]):
    """Runs the indicator."""
    indicator = indicator_type(params)
    validator = validator_type(params["validation"])
    archiver = archiver_type(params)

    indicator.run()
    validation_report = validator.validate()
    if validation_report.success():         
        archiver.archive()
