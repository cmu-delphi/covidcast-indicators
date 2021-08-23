# -*- coding: utf-8 -*-
"""Functions for pulling data from the USAFacts website."""
from datetime import date
import hashlib
from logging import Logger
import os

import numpy as np
import pandas as pd
import requests

# Columns to drop the the data frame.
DROP_COLUMNS = [
    "countyfips",
    "county name",
    "state",
    "statefips"
]

def fetch(url: str, cache: str) -> pd.DataFrame:
    """Handle network I/O for fetching raw input data file.

    This is necessary because for some reason pd.read_csv is generating
    403:Forbidden on the new URLs.
    """
    r = requests.get(url)
    r.raise_for_status()
    datestamp = date.today().strftime('%Y%m%d')
    name = url.split('/')[-1].replace('.csv','')
    os.makedirs(cache, exist_ok=True)
    filename = os.path.join(cache, f"{datestamp}_{name}.csv")
    with open(filename, "w") as f:
        f.write(r.text)
    return pd.read_csv(filename)


def pull_usafacts_data(base_url: str, metric: str, logger: Logger, cache: str=None) -> pd.DataFrame:
    """Pull the latest USA Facts data, and conform it into a dataset.

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

    We filter the data such that we only keep rows with valid FIPS, or "FIPS"
    codes defined under the exceptions of the README.  The current  exceptions
    include:

    # - 6000: Grand Princess Cruise Ship
    # - 2270: Wade Hampton Census Area in AK, but no cases/deaths were assigned
    # - 0: statewise unallocated
    # - 1: New York City Unallocated/Probable (only exists for NYC)

    PS:  No information for PR
    Parameters
    ----------
    base_url: str
        Base URL for pulling the USA Facts data
    metric: str
        One of 'confirmed' or 'deaths'. The keys of base_url.
    logger: Logger
    cache: str
        Directory where downloaded csvs should be stashed.

    Returns
    -------
    pd.DataFrame
        Dataframe as described above.
    """
    # Read data
    df = fetch(base_url.format(metric=metric), cache)
    date_cols = [i for i in df.columns if i.startswith("2")]
    logger.info("data retrieved from source",
                metric=metric,
                num_rows=df.shape[0],
                num_cols=df.shape[1],
                min_date=min(date_cols),
                max_date=max(date_cols),
                checksum=hashlib.sha256(pd.util.hash_pandas_object(df).values).hexdigest())
    df.columns = [i.lower() for i in df.columns]
    # Clean commas in count fields in case the input file included them
    df[df.columns[4:]] = df[df.columns[4:]].applymap(
        lambda x: int(x.replace(",", "")) if isinstance(x, str) else x)
    # Check missing FIPS
    null_mask = pd.isnull(df["countyfips"])
    assert null_mask.sum() == 0

    unexpected_columns = [x for x in df.columns if "Unnamed" in x]
    unexpected_columns.extend(DROP_COLUMNS)

    # Assign Grand Princess Cruise Ship a special FIPS 90000
    # df.loc[df["FIPS"] == 6000, "FIPS"] = 90000
    # df.loc[df["FIPS"] == 6000, "stateFIPS"] = 90

    # Ignore Grand Princess Cruise Ship and Wade Hampton Census Area in AK
    df = df[
        (df["countyfips"] != 6000)
        & (df["countyfips"] != 2270)
    ]

    # Change FIPS from 0 to XX000 for statewise unallocated cases/deaths
    unassigned_index = (df["countyfips"] == 0)
    df.loc[unassigned_index, "countyfips"] = df["statefips"].loc[unassigned_index].values * 1000

    # Conform FIPS
    df["fips"] = df["countyfips"].apply(lambda x: f"{int(x):05d}")



    # Drop unnecessary columns (state is pre-encoded in fips)
    try:
        df.drop(DROP_COLUMNS, axis=1, inplace=True)
    except KeyError as e:
        raise ValueError(
            "Tried to drop non-existent columns. The dataset "
            "schema may have changed.  Please investigate and "
            "amend DROP_COLUMNS."
        ) from e
    # Check that columns are either FIPS or dates
    try:
        columns = list(df.columns)
        columns.remove("fips")
        # Detects whether there is a non-date string column -- not perfect
        # USAFacts has used both / and -, so account for both cases.
        _ = [int(x.replace("/", "").replace("-", "")) for x in columns]
    except ValueError as e:
        raise ValueError(
            "Detected unexpected column(s) "
            "after dropping DROP_COLUMNS. The dataset "
            "schema may have changed. Please investigate and "
            "amend DROP_COLUMNS."
        ) from e
    # Reshape dataframe
    df = df.melt(
        id_vars=["fips"],
        var_name="timestamp",
        value_name="cumulative_counts",
    )
    # timestamp: str -> datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    # Add a dummy first row here on day before first day
    min_ts = min(df["timestamp"])
    df_dummy = df.loc[df["timestamp"] == min_ts].copy()
    df_dummy.loc[:, "timestamp"] = min_ts - pd.Timedelta(days=1)
    df_dummy.loc[:, "cumulative_counts"] = 0
    df = pd.concat([df_dummy, df])
    # Obtain new_counts
    df.sort_values(["fips", "timestamp"], inplace=True)
    df["new_counts"] = df["cumulative_counts"].diff()  # 1st discrete difference
    # Handle edge cases where we diffed across fips
    mask = df["fips"] != df["fips"].shift(1)
    df.loc[mask, "new_counts"] = np.nan
    df.reset_index(inplace=True, drop=True)

    # Final sanity checks
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
    return df.loc[
        df["timestamp"] >= min_ts,
        [  # Reorder
            "fips",
            "timestamp",
            "new_counts",
            "cumulative_counts",
        ],
    ]
