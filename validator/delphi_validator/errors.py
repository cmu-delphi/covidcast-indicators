# -*- coding: utf-8 -*-
"""
Custom validator exceptions.
"""
from dataclasses import dataclass

class APIDataFetchError(Exception):
    """Exception raised when reading API data goes wrong.

    Attributes:
        custom_msg -- parameters which caused the error
    """

    def __init__(self, custom_msg):
        self.custom_msg = custom_msg
        super().__init__(self.custom_msg)

    def __str__(self):
        return '{}'.format(self.custom_msg)

@dataclass
class ValidationFailure:
    """Structured report of single validation failure."""
    # Which check the failure came from
    check_name: str
    # Data source of the failure
    data_name: str
    # Additional context about the failure
    message: str

    def is_suppressed(self, suppressed_errors):
        """Determine whether the failure should be suppressed.

        Parameters
        ----------
        errors_to_suppress: Set[Tuple[str]]
            set of (check_name, data_name) tuples to ignore.
        """
        return (self.check_name, self.data_name) in suppressed_errors

    def __str__(self):
        return f"{self.check_name} failed for {self.data_name}: {self.message}"
