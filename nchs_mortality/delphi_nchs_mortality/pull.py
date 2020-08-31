# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
from sodapy import Socrata

def pull_nchs_mortality_data(token: str, map_df: pd.DataFrame) -> pd.DataFrame:
    """Pulls the latest NCHS Mortality data, and conforms it into a dataset

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
    map_df: pd.DataFrame
        Read from static file "state_pop.csv".

    Returns
    -------
    pd.DataFrame
        Dataframe as described above.
    """
    # Constants
    KEEP_COLUMNS = ['covid_deaths', 'total_deaths', 'pneumonia_deaths',
                    'pneumonia_and_covid_deaths', 'influenza_deaths',
                    'pneumonia_influenza_or_covid_19_deaths']
    TYPE_DICT = {key: float for key in KEEP_COLUMNS}
    TYPE_DICT["timestamp"] = 'datetime64[ns]'

    # Pull data from Socrata API
    client = Socrata("data.cdc.gov", token)
    results = client.get("r8kw-7aab", limit=10**10)
    df = pd.DataFrame.from_records(results).rename(
            {"start_week": "timestamp"}, axis=1)

    # Check missing start_week == end_week
    try:
        assert sum(df["timestamp"] != df["end_week"]) == 0
    except AssertionError:
        raise ValueError(
            "end_week is not always the same as start_week, check the raw file"
        )

    df = df.astype(TYPE_DICT)
    df = df[df["state"] != "United States"]
    df.loc[df["state"] == "New York City", "state"] = "New York"

    state_list = df["state"].unique()
    date_list = df["timestamp"].unique()
    index_df = pd.MultiIndex.from_product(
        [state_list, date_list], names=['state', 'timestamp']
    )
    df = df.groupby(
            ["state", "timestamp"]).sum().reindex(index_df).reset_index()

    # Final sanity checks
    days_by_states = df.groupby("state").count()["covid_deaths"].unique()
    unique_days = df["timestamp"].unique()
    # each FIPS has same number of rows
    if (len(days_by_states) > 1) or (days_by_states[0] != len(unique_days)):
        raise ValueError("Differing number of days by fips")
    min_timestamp = min(unique_days)
    max_timestamp = max(unique_days)
    n_days = (max_timestamp - min_timestamp) / np.timedelta64(1, 'D') / 7 + 1
    if n_days != len(unique_days):
        raise ValueError(
            f"Not every day between {min_timestamp} and "
            "{max_timestamp} is represented."
        )

    # Add population info
    KEEP_COLUMNS.extend(["timestamp", "geo_id", "population"])
    try:
        df = df.merge(map_df, on="state")[KEEP_COLUMNS]
    except KeyError:
        raise ValueError("Expected column(s) missed, The dataset "
            "schema may have changed. Please investigate and "
            "amend the code.")
    return df
