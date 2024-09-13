"""
Utility for converting dates to a format accepted by epidata api.
"""

from datetime import datetime

import pandas as pd
from epiweeks import Week


def _date_to_api_string(d: datetime, time_type: str = "day") -> str:
    """Convert a date object to a YYYYMMDD or YYYYMM string expected by the API."""
    # pylint: disable=R1705
    if time_type == "day":
        return d.strftime("%Y%m%d")
    elif time_type == "week":
        return Week.fromdate(d).cdcformat()
    raise ValueError(f"Unknown time_type: {time_type}")


def _parse_datetimes(df: pd.DataFrame, col: str, date_format: str = "%Y%m%d") -> pd.Series:
    """Convert a DataFrame date or epiweek column into datetimes.

    Assumes the column is string type. Dates are assumed to be in the YYYYMMDD
    format by default. Weeks are assumed to be in the epiweek CDC format YYYYWW
    format and return the date of the first day of the week.
    """
    df[col] = df[col].astype("str")

    def parse_row(row):
        if row["time_type"] == "day":
            return pd.to_datetime(row[col], format=date_format)
        if row["time_type"] == "week":
            return pd.to_datetime(Week(int(row[col][:4]), int(row[col][-2:])).startdate())
        return row[col]

    return df.apply(parse_row, axis=1)
