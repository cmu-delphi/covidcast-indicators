# -*- coding: utf-8 -*-
"""Functions to pull data from JHU website."""

import pandas as pd
import numpy as np
from delphi_utils import GeoMapper


def download_data(base_url: str, metric: str) -> pd.DataFrame:
    """
    Download and format JHU data.

    Downloads the data from the JHU repo, extracts the UID and the date columns, and
    enforces the date datatype on the the time column.
    """
    # Read data
    df = pd.read_csv(base_url.format(metric=metric))
    # Keep the UID and the time series columns only
    # The regex filters for columns with the date format MM-DD-YY or M-D-YY
    df = df.filter(regex=r"\d{1,2}\/\d{1,2}\/\d{2}|UID").melt(
        id_vars=["UID"], var_name="timestamp", value_name="cumulative_counts"
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def create_diffs_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute pairwise differences of cumulative values to get incidence.

    Using the cumulative_counts column from the dataframe, partitions the dataframe
    into separate time-series based on fips, and then computes pairwise differences
    of the cumulative values to get the incidence values. Boundary cases are handled
    by zero-filling the day prior.
    """
    # Take time-diffs in each geo_code partition
    df = df.set_index(["fips", "timestamp"])
    df["new_counts"] = df.groupby(level=0)["cumulative_counts"].diff()
    # Fill the NA value for the first date of each partition with the cumulative value that day
    # (i.e. pretend the cumulative count the day before was 0)
    na_value_mask = df["new_counts"].isna()
    df.loc[na_value_mask, "new_counts"] = df.loc[na_value_mask, "cumulative_counts"]
    df = df.reset_index()
    return df


def sanity_check_data(df: pd.DataFrame) -> pd.DataFrame:
    """Perform a final set of sanity checks on the data."""
    days_by_fips = df.groupby("fips").count()["cumulative_counts"].unique()
    unique_days = df["timestamp"].unique()

    # each FIPS has same number of rows
    if (len(days_by_fips) > 1) or (days_by_fips[0] != len(unique_days)):
        raise ValueError("Differing number of days by fips")

    min_timestamp = min(unique_days)
    max_timestamp = max(unique_days)
    n_days = (max_timestamp - min_timestamp) / np.timedelta64(1, "D") + 1
    if n_days != len(unique_days):
        raise ValueError(
            f"Not every day between {min_timestamp} and "
            "{max_timestamp} is represented."
        )


def pull_jhu_data(base_url: str, metric: str, gmpr: GeoMapper) -> pd.DataFrame:
    """Pull the latest Johns Hopkins CSSE data, and conform it into a dataset.

    The output dataset has:

    - Each row corresponds to (County, Date), denoted (FIPS, timestamp)
    - Each row additionally has a column `new_counts` corresponding to the new
      new_counts (either `confirmed` cases or `deaths`), and a column
      `cumulative_counts`, correspond to the aggregate metric from January 22nd
      (as of April 27th) until the latest date.

    Note that the raw dataset gives the `cumulative_counts` metric, from which
    we compute `new_counts` by taking first differences.  Hence, `new_counts`
    may be negative.  This is wholly dependent on the quality of the raw
    dataset.

    We filter the data such that we only keep rows with valid FIPS or "FIPS"
    codes defined under the exceptions of the README.

    Parameters
    ----------
    base_url: str
        Base URL for pulling the JHU CSSE data.
    metric: str
        One of 'confirmed' or 'deaths'.
    gmpr: GeoMapper
        An instance of the geomapping utility.

    Returns
    -------
    pd.DataFrame
        Dataframe as described above.
    """
    df = download_data(base_url, metric)

    gmpr = GeoMapper()
    df = gmpr.replace_geocode(
        df, "jhu_uid", "fips", from_col="UID", date_col="timestamp"
    )

    # Merge in population, set population as NAN for fake fips
    df = gmpr.add_population_column(df, "fips")
    df = create_diffs_column(df)

    # Final sanity checks
    sanity_check_data(df)

    # Reorder columns
    df = df[["fips", "timestamp", "population", "new_counts", "cumulative_counts"]]
    return df
