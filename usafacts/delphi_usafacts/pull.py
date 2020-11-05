# -*- coding: utf-8 -*-
"""Functions for pulling data from the USAFacts website."""
import numpy as np
import pandas as pd
from delphi_utils import GeoMapper

# Columns to drop the the data frame.
DROP_COLUMNS = [
    "FIPS",
    "County Name",
    "State",
    "stateFIPS"
]


def pull_usafacts_data(base_url: str, metric: str, geo_mapper: GeoMapper) -> pd.DataFrame:
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
    geo_mapper: GeoMapper
        GeoMapper object with population info.

    Returns
    -------
    pd.DataFrame
        Dataframe as described above.
    """
    # Read data
    df = pd.read_csv(base_url.format(metric=metric)).rename({"countyFIPS":"FIPS"}, axis=1)
    # Check missing FIPS
    null_mask = pd.isnull(df["FIPS"])
    assert null_mask.sum() == 0

    unexpected_columns = [x for x in df.columns if "Unnamed" in x]
    unexpected_columns.extend(DROP_COLUMNS)

    # Assign Grand Princess Cruise Ship a special FIPS 90000
    # df.loc[df["FIPS"] == 6000, "FIPS"] = 90000
    # df.loc[df["FIPS"] == 6000, "stateFIPS"] = 90

    # Ignore Grand Princess Cruise Ship and Wade Hampton Census Area in AK
    df = df[
        (df["FIPS"] != 6000)
        & (df["FIPS"] != 2270)
    ]

    # Change FIPS from 0 to XX000 for statewise unallocated cases/deaths
    unassigned_index = (df['FIPS'] == 0)
    df.loc[unassigned_index, "FIPS"] = df["stateFIPS"].loc[unassigned_index].values * 1000

    # Conform FIPS
    df["fips"] = df["FIPS"].apply(lambda x: f"{int(x):05d}")

    # The FIPS code 00001 is a dummy for unallocated NYC data.  It doesn't have
    # a corresponding population entry in the GeoMapper so it will be dropped
    # in the call to `add_population_column()`.  We pull it out here to
    # reinsert it after the population data is added.
    nyc_dummy_row = df[df["fips"] == "00001"]
    assert len(nyc_dummy_row) == 1

    # Merge in population LOWERCASE, consistent across confirmed and deaths
    # Population for unassigned cases/deaths is NAN
    df = geo_mapper.add_population_column(df, "fips")
    df = df.append(nyc_dummy_row, ignore_index=True)

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
        columns.remove("population")
        # Detects whether there is a non-date string column -- not perfect
        _ = [int(x.replace("/", "")) for x in columns]
    except ValueError as e:
        raise ValueError(
            "Detected unexpected column(s) "
            "after dropping DROP_COLUMNS. The dataset "
            "schema may have changed. Please investigate and "
            "amend DROP_COLUMNS."
        ) from e
    # Reshape dataframe
    df = df.melt(
        id_vars=["fips", "population"],
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
            "population",
            "new_counts",
            "cumulative_counts",
        ],
    ]
