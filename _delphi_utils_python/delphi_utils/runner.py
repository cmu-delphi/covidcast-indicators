"""Indicator running utilities."""

import abc
from logging import Logger
from typing import Any, Dict, List, Type

from .archive import ArchiveDiffer

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


class Validator(abc.ABC):
    pass


class IndicatorRunner:
    """Class to run indicators."""
    def __init__(self,
                 name:  str,
                 indicator: Type[Indicator],
                 validators:  List[Type[Validator]],
                 archiver:  Type[ArchiveDiffer],
                 logger:  Logger,
                 params: Dict[str, Any]):
        self.name = name
        self.indicator = indicator(params)
        self.validators = [v(params) for v in validators]
        self.archiver = archiver(params)
        self.logger = logger

    def _run_stage(self, stage):
        """Helper function to run a stage and handle errors."""
        try:
            stage.run()
        except Exception as e:
            self.logger.log(f"{self.name}.{stage.name} failed with error {e}")
            raise e

    def run(self):
        """Runs the indicator."""
        self._run_stage(self.indicator)
        for validator in self.validators:
            self._run_stage(validator)
        self._run_stage(self.archiver)
