"""Data container classes for holding sensor configurations and data needed for fusion."""

from dataclasses import dataclass, field
from typing import List, Union

from numpy import ndarray, nan, nanmean, isnan
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

    geo_value: str = None
    geo_type: str = None
    dates: List[int] = field(default_factory=lambda: [])
    values: Union[List, ndarray] = field(default_factory=lambda: [])

    def __post_init__(self):
        """Input validation."""
        if (self.dates is not None and self.values is not None) and \
                (len(self.dates) != len(self.values)):
            raise ValueError("Length of dates and values differs.")

        if len(set(self.dates)) < len(self.dates):
            raise ValueError("Duplicate dates not allowed.")

    def add_data(self, date, value):
        """Append a date and value to existing attributes.

        Safer than appending individually since the two lists shouldn't have different lengths.
        """
        self.dates.append(date)
        self.values.append(value)

    @property
    def empty(self):
        """Check if there is no stored data in the class."""
        return not self.dates and not self.values

    def get_value(self, date: int) -> float:
        """Return value for a given date or nan if not available."""
        try:
            return self.values[self.dates.index(date)]
        except ValueError:
            return nan

    def get_data_range(self,
                       start_date: int,
                       end_date: int,
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
        all_dates = [int(i.strftime("%Y%m%d")) for i in date_range(str(start_date), str(end_date))]
        out_values = []
        for day in all_dates:
            out_values.append(self.get_value(day))
        if imputation_method is None or not out_values:
            return out_values
        if imputation_method == "mean":
            mean = nanmean(out_values)
            out_values = [i if not isnan(i) else mean for i in out_values]
            return out_values
        raise ValueError("Invalid imputation method. Must be None or 'mean'")
