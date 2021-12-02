"""
Load CHC data with multiple updates.
"""
# standard
from datetime import timedelta, datetime

# third party
import numpy as np
import pandas as pd
from delphi_utils import GeoMapper

# first party
from .config import Config

gmpr = GeoMapper()

def date_range(dropdate, n_days):
    """Generate list of dates starting from dropdate-n_days to the dropdate.
    """
    return [dropdate-timedelta(days=x) for x in range(n_days-1, -1, -1)]

def load_chng_data(filepath, cache_dir, date_list, count_type,
                   col_names, col_types, counts_col):
    """Load in and aggregate daily count data from Change.

    Args:
        filepath: path to aggregated data
        cache_dir: the directory to the cache folder
        date_list: list of data drop dates (datetime object)
        count_type: "Covid" or "Denom"
        col_names: column names of data
        col_types: column types of data
        counts_col: name of column containing counts

    Returns:
        cleaned dataframe
    """
    count_flag = False
    date_flag = False
    geo_flag = False
    for n in col_names:
        if n == counts_col:
            count_flag = True
        elif n == Config.DATE_COL:
            date_flag = True
        elif n == Config.GEO_COL:
            geo_flag = True
    assert count_flag, "counts_col must be present in col_names"
    assert date_flag, f"'{Config.DATE_COL}' must be present in col_names"
    assert geo_flag, "'fips' must be present in col_names"

    pdList = []
    for dropdate in date_list:
        try:
            data = pd.read_csv(
                filepath%(cache_dir, dropdate.strftime("%Y%m%d"), count_type),
                sep=",",
                header=None,
                names=col_names,
                dtype=col_types,
            ).dropna()

            data[Config.DATE_COL] = \
                pd.to_datetime(data[Config.DATE_COL],errors="coerce")

            # restrict to start and end date
            data = data[
                (data[Config.DATE_COL] >= Config.FIRST_DATA_DATE) &
                # Ignore data with lag >= 150
                (data[Config.DATE_COL] >= dropdate - timedelta(days=180)) &
                (data[Config.DATE_COL] < dropdate)
                ]

            # counts between 1 and 3 are coded as "3 or less", we convert to 1
            data.loc[data[counts_col] == "3 or less", counts_col] = "1"
            data[counts_col] = data[counts_col].astype(int)

            assert (
                (data[counts_col] >= 0).all().all()
            ), "Counts must be nonnegative"

            # aggregate age groups (so data is unique by date and base geography)
            data = data.groupby([Config.GEO_COL, Config.DATE_COL]).sum().reset_index()
            data.dropna(inplace=True)  # drop rows with any missing entries

            data["lag"] = (dropdate - data[Config.DATE_COL]).dt.days
            data["issue_date"] = dropdate

            pdList.append(data)
        except FileNotFoundError:
            pass

    combined_data = pd.concat(pdList)
    combined_data = combined_data.loc[
        combined_data[Config.GEO_COL].isin(gmpr.get_geo_values("fips"))]

    return combined_data


def get_weekofmonth(x):
    """Return which week it is in a month.

    It will return 0 for the first week of a month. The last few days in the
    fifth week in some months will be merged into the first week.

    Args:
        x: a date (datetime object)

    Returns:
        which week it is in a month
    """
    yy, mm = x.year, x.month
    firstdayofmonth = datetime(yy, mm, 1).weekday()
    return ((x.day + firstdayofmonth) // 7) % 4
