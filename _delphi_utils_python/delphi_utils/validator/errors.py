# -*- coding: utf-8 -*-
"""Custom validator exceptions."""
import datetime as dt
from typing import Union, Optional

class APIDataFetchError(Exception):
    """Exception raised when reading API data goes wrong.

    Attributes:
        custom_msg -- parameters which caused the error
    """

    def __init__(self, custom_msg):
        """Initialize the APIDataFetchError with `custom_msg`."""
        self.custom_msg = custom_msg
        super().__init__(self.custom_msg)

    def __str__(self):
        """Extract the message."""
        return '{}'.format(self.custom_msg)

class ValidationFailure:
    """Structured report of single validation failure."""

    def __init__(self,
                 check_name: Optional[str]=None,
                 date: Optional[Union[str, dt.date]]=None,
                 geo_type: Optional[str]=None,
                 signal: Optional[str]=None,
                 message: str="",
                 filename: Optional[str]=None):
        """Initialize a ValidationFailure.

        Parameters
        ----------
        check_name: Optional[str]
            Name of check at which the failure happened.  A value of `None` is used to express all
            possible checks with a given `date`, `geo_type`, and/or `signal`.
        date: Optional[Union[str, dt.date]]
            Date corresponding to the data over which the failure happened.
            Strings are interpretted in ISO format ("YYYY-MM-DD").
            When `None`, the failure is not tied to any date.
        geo_type: Optional[str]
            Geo resolution of the data at which the failure occurred.
            When `None`, the failure is not tied to any geo resolution.
        signal: Optional[str]
            COVIDcast signal for which the failure occurred.
            When `None`, the failure is not tied to any signal.
        message: str
            Additional context about the failure
        filename: Optional[str]
            Filename from which the failing data came.
            When provided, any values passed to the `date`, `geo_type`, and `signal` parameters are
            ignored and instead derived from this parameter, under the assumption that the filename
            is formatted as "{date}_{geo_type}_{signal}.{extension}", where `date` is in the format
            "YYYYMMDD".

        Attributes
        ----------
        check_name: str
            See above.
        date: Optional[dt.date]
            See above.
        geo_type: Optional[str]
            See above.
        signal: Optional[str]
            See above.
        message: str
            See above.
        """
        self.check_name = check_name
        self.message = message
        if filename:
            pieces = filename.split(".")[0].split("_", maxsplit=2)
            assert len(pieces) == 3, '`filename` argument expected to be in "{date}_{geo_type}_'\
                                     '{signal}.{extension}" format'
            try:
                date = dt.datetime.strptime(pieces[0], "%Y%m%d").date()
            except ValueError as e:
                raise ValueError('date in `filename` must be in "YYYYMMDD" format') from e
            geo_type = pieces[1]
            signal = pieces[2]
        if isinstance(date, str):
            date = dt.date.fromisoformat(date)
        self.date = date
        self.geo_type = geo_type
        self.signal = signal

    def __eq__(self, other):
        """Compare this object with ValidationFailure `other` for equality.

        Two ValidationFailures are considered equal if their `check_name`, `date`, `geo_type`, and
        `signal` attributes correspondingly match.  A value of `None` in any of these attributes is
        considered to match any corresponding value on the other ValidationFailure.
        """
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
            Because we allow `None` values as wildcards, we cannot assume errors that are equal
            will hash to the same values.  Thus, we need to use a list rather than a set here.
        """
        return self in suppressed_errors

    def __str__(self):
        """Summarize context of failure."""
        date_str = "*" if self.date is None else self.date.isoformat()
        return f"{self.check_name} failed for {self.signal} at resolution {self.geo_type} on "\
               f"{date_str}: {self.message}"
