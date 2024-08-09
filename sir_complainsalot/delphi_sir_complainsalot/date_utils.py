from datetime import datetime

from epiweeks import Week
import pandas as pd

def _date_to_api_string(d: datetime, time_type: str = "day") -> str:
    """Convert a date object to a YYYYMMDD or YYYYMM string expected by the API."""
    if time_type == "day":
        return d.strftime("%Y%m%d")
    elif time_type == "week":
        return Week.fromdate(d).cdcformat()
    raise ValueError(f"Unknown time_type: {time_type}")

def _parse_datetimes(df: pd.DataFrame, col: str, time_type: str, date_format: str = "%Y%m%d") -> pd.DataFrame:
    """Convert a DataFrame date or epiweek column into datetimes.

    Assumes the column is string type. Dates are assumed to be in the YYYYMMDD
    format by default. Weeks are assumed to be in the epiweek CDC format YYYYWW
    format and return the date of the first day of the week.
    """
    if time_type == "day":
        df[col] = pd.to_datetime(df[col], format=date_format)
        return df
    if time_type == "week":
        df[col] = df[col].apply(lambda x: Week(int(x[:4]), int(x[-2:])).startdate())
        df[col] = pd.to_datetime(df[col])
        return df
    raise ValueError(f"Unknown time_type: {time_type}")
