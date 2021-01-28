"""Data container classes for holding sensor configurations and data needed for fusion."""

from dataclasses import dataclass
from typing import List
from datetime import date

from numpy import nan, nanmean, isnan
from pandas import date_range


@dataclass(frozen=True)
class SensorConfig:
    """Dataclass for specifying a sensor's name, number of lag days, and origin source/signal."""

    source: str
    signal: str
    name: str
    lag: int


@dataclass
class LocationSeries:
    """Data class for holding time series data specific to a single location."""

    def __init__(self,
                 geo_value: str = None,
                 geo_type: str = None,
                 dates: List[date] = None,
                 values: List[float] = None):
        """Initialize LocationSeries."""
        has_data = dates is not None and values is not None
        if has_data and (len(dates) != len(values)):
            raise ValueError("Length of dates and values differs.")
        if has_data and len(set(dates)) < len(dates):
            raise ValueError("Duplicate dates not allowed.")
        self.geo_value = geo_value
        self.geo_type = geo_type
        self.data = dict(zip(dates, values)) if has_data else {}

    def add_data(self, date, value, overwrite=False):
        """Append a date and value to existing attributes.

        Safer than appending individually since the two lists shouldn't have different lengths.
        """
        if self.data.get(date) and not overwrite:
            raise ValueError("Date already exists in LocationSeries")
        self.data[date] = value

    @property
    def empty(self):
        """Check if there is no stored data in the class."""
        return not self.dates and not self.values

    @property
    def dates(self):
        """Check if there is no stored data in the class."""
        return self.data.keys()

    @property
    def values(self):
        """Check if there is no stored data in the class."""
        return self.data.values()

    def get_data_range(self,
                       start_date: date,
                       end_date: date,
                       imputation_method: str = None) -> List[float]:
        """
        Return value of LocationSeries between two dates with optional imputation.

        Parameters
        ----------
        start_date
            First day to include in range.
        end_date
            Last day to include in range.
        imputation_method
            Optional type of imputation to conduct. Currently only "mean" is supported.

        Returns
        -------
            List of values, one for each day in the range.
        """
        if start_date < min(self.dates) or end_date > max(self.dates):
            raise ValueError(f"Data range must be within existing dates "
                             f"{min(self.dates)}-{max(self.dates)}.")
        all_dates = date_range(start_date, end_date)
        out_values = [self.data.get(day.date(), nan) for day in all_dates]
        if imputation_method is None or not out_values:
            return out_values
        if imputation_method == "mean":
            mean = nanmean(out_values)
            out_values = [i if not isnan(i) else mean for i in out_values]
            return out_values
        raise ValueError("Invalid imputation method. Must be None or 'mean'")
