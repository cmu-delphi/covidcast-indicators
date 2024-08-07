from datetime import datetime
from typing import Union

from epiweeks import Week
import pandas as pd
def _date_to_api_string(date: datetime.date, time_type: str = "day") -> str:  # pylint: disable=W0621
    """Convert a date object to a YYYYMMDD or YYYYMM string expected by the API."""
    if time_type == "day":
        date_str = date.strftime("%Y%m%d")
    elif time_type == "week":
        date_str = Week.fromdate(date).cdcformat()
    return date_str

def _parse_datetimes(date_int: str, time_type: str, date_format: str = "%Y%m%d") -> Union[pd.Timestamp, None]:
    """Convert a date or epiweeks string into timestamp objects.

    Datetimes (length 8) are converted to their corresponding date, while epiweeks (length 6)
    are converted to the date of the start of the week. Returns nan otherwise

    Epiweeks use the CDC format.

    date_int: Int representation of date.
    time_type: The temporal resolution to request this data. Most signals
      are available at the "day" resolution (the default); some are only
      available at the "week" resolution, representing an MMWR week ("epiweek").
    date_format: String of the date format to parse.
    :returns: Timestamp.
    """
    date_str = str(date_int)
    if time_type == "day":
        return pd.to_datetime(date_str, format=date_format)
    if time_type == "week":
        epiwk = Week(int(date_str[:4]), int(date_str[-2:]))
        return pd.to_datetime(epiwk.startdate())
    return None