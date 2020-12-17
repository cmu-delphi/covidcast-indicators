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
    check_name: str
    data_name: str
    message: str

    def is_suppressed(self, suppressed_errors):
        return (self.check_name, self.data_name) in suppressed_errors

    def __str__(self):
        return f"{self.check_name} failed for {self.data_name}: {self.message}"
