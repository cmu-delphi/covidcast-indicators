# -*- coding: utf-8 -*-
"""Functions for pulling NCHS mortality data API."""

from typing import Optional

import numpy as np
import pandas as pd
from sodapy import Socrata

from delphi_utils.geomap import GeoMapper

from .constants import METRICS, RENAME, NEWLINE

def standardize_columns(df):
    """Rename columns to comply with a standard set.

    NCHS has changed column names a few times, so this will help us maintain
    backwards-compatibility without the processing code getting all gnarly.
    """
    rename_pairs = [(from_col, to_col) for (from_col, to_col) in RENAME
                     if from_col in df.columns]
    return df.rename(columns=dict(rename_pairs))


def pull_nchs_mortality_data(token: str, test_file: Optional[str]=None):
    """Pull the latest NCHS Mortality data, and conforms it into a dataset.

    The output dataset has:

    - Each row corresponds to (State, Week), denoted (geo_id, timestamp)
    - Each row additionally has columns 'covid_deaths', 'total_deaths',
       'percent_of_expected_deaths', 'pneumonia_deaths',
       'pneumonia_and_covid_deaths', 'influenza_deaths',
       'pneumonia_influenza_or_covid_19_deaths' correspond to the aggregate
       metric from Feb. 1st until the latest date.

    # New York City would be included in New York State

    Parameters
    ----------
    token: str
        My App Token for pulling the NCHS mortality data
    test_file: Optional[str]
        When not null, name of file from which to read test data

    Returns
    -------
    pd.DataFrame
        Dataframe as described above.
    """
    # Constants
    keep_columns = METRICS.copy()
    type_dict = {key: float for key in keep_columns}
    type_dict["timestamp"] = 'datetime64[ns]'

    if test_file:
        df = pd.read_csv("./test_data/%s"%test_file)
    else:
        # Pull data from Socrata API
        client = Socrata("data.cdc.gov", token)
        results = client.get("r8kw-7aab", limit=10**10)
        df = pd.DataFrame.from_records(results)
        # drop "By Total" rows
        df = df[df["group"].transform(str.lower) == "by week"]

    df = standardize_columns(df)

    if "end_date" in df.columns:
        # Check missing week_ending_date == end_date
        try:
            assert all(df["week_ending_date"] == df["end_date"])
        except AssertionError as exc:
            raise ValueError(
                "week_ending_date is not always the same as end_date, check the raw file"
            ) from exc
    else:
        # Check missing start_week == end_week
        try:
            assert all(df["timestamp"] == df["end_week"])
        except AssertionError as exc:
            raise ValueError(
                "end_week is not always the same as start_week, check the raw file"
            ) from exc

    try:
        df = df.astype(type_dict)
    except KeyError as exc:
        raise ValueError(f"""
Expected column(s) missed, The dataset schema may
have changed. Please investigate and amend the code.

Columns needed:
{NEWLINE.join(type_dict.keys())}

Columns available:
{NEWLINE.join(df.columns)}
""") from exc

    # Drop rows for locations outside US
    df = df[df["state"] != "United States"]
    df = df.loc[:, keep_columns + ["timestamp", "state"]].set_index("timestamp")

    # NCHS considers NYC as an individual state, however, we want it included
    # in NY. If values are nan for both NYC and NY, the aggreagtion should
    # also have NAN.
    df_ny = df.loc[df["state"] == "New York", :].drop("state", axis=1)
    df_nyc = df.loc[df["state"] == "New York City", :].drop("state", axis=1)
    # Get mask df to ignore cells where both of them have NAN values
    mask = (df_ny[keep_columns].isnull().values \
            & df_nyc[keep_columns].isnull().values)
    df_ny = df_ny.append(df_nyc).groupby("timestamp").sum().where(~mask, np.nan)
    df_ny["state"] = "New York"
    # Drop NYC and NY in the full dataset
    df = df.loc[~df["state"].isin(["New York", "New York City"]), :]
    df = df.append(df_ny).reset_index().sort_values(["state", "timestamp"])
    # Add population info
    keep_columns.extend(["timestamp", "geo_id", "population"])
    gmpr = GeoMapper()
    df = gmpr.add_population_column(df, "state_name", geocode_col="state")
    df = gmpr.add_geocode(df, "state_name", "state_id", from_col="state", new_col="geo_id")
    return df[keep_columns]
