# -*- coding: utf-8 -*-
"""
Custom validator exceptions.
"""
# from dataclasses import dataclass
import datetime as dt
from typing import Optional

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

class ValidationFailure:
    """Structured report of single validation failure."""
    def __init__(self,
                 check_name: str,
                 date: Optional[dt.date]=None,
                 geo_type: Optional[str]=None,
                 signal: Optional[str]=None,
                 message: str="",
                 filename: Optional[str]=None):
        """
            # Which check the failure came from
    check_name: str
    # Date of the failure
    date: Optional[dt.date]
    # Geo resolution of the failure
    geo_type: Optional[str]
    # Signal name of the failure
    signal: Optional[str]
    # Additional context about the failure
    message: str."""
        self.check_name = check_name
        self.message = message
        if filename:
            pieces = filename.split(".")[0].split("_", maxsplit=2)
            assert len(pieces) == 3
            date = dt.datetime.strptime(pieces[0], "%Y%m%d").date()
            geo_type = pieces[1]
            signal = pieces[2]
        if isinstance(date, str):
            date = dt.date.fromisoformat(date)
        self.date = date
        self.geo_type = geo_type
        self.signal = signal

    def __eq__(self, other):
        def match_with_wildcard(x, y):
            """Determine if x and y are equal or None."""
            return x is None or y is None or x == y
        return match_with_wildcard(self.check_name, other.check_name) and\
            match_with_wildcard(self.date, other.date) and\
            match_with_wildcard(self.geo_type, other.geo_type) and\
            match_with_wildcard(self.signal, other.signal)

    def is_suppressed(self, suppressed_errors):
        """Determine whether the failure should be suppressed.

        Parameters
        ----------
        errors_to_suppress: List[ValidationFailure]
            set of data sources to ignore.
        """
        return self in suppressed_errors

    def __str__(self):
        date_str = "*" if self.date is None else self.date.isoformat()
        return f"{self.check_name} failed for {self.signal} at resolution {self.geo_type} on "\
               f"{date_str}: {self.message}"
