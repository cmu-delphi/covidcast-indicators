# -*- coding: utf-8 -*-
"""Functions for pulling NCHS mortality data API."""
import pandas as pd
from sodapy import Socrata

def pull_nchs_mortality_data(token: str, map_df: pd.DataFrame, test_mode: str):
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
    map_df: pd.DataFrame
        Read from static file "state_pop.csv".
    test_mode:str
        Check whether to run in a test mode

    Returns
    -------
    pd.DataFrame
        Dataframe as described above.
    """
    # Constants
    keep_columns = ['covid_deaths', 'total_deaths',
                    'percent_of_expected_deaths', 'pneumonia_deaths',
                    'pneumonia_and_covid_deaths', 'influenza_deaths',
                    'pneumonia_influenza_or_covid_19_deaths']
    type_dict = {key: float for key in keep_columns}
    type_dict["timestamp"] = 'datetime64[ns]'

    if test_mode == "":
        # Pull data from Socrata API
        client = Socrata("data.cdc.gov", token)
        results = client.get("r8kw-7aab", limit=10**10)
        df = pd.DataFrame.from_records(results).rename(
                {"start_week": "timestamp"}, axis=1)
    else:
        df = pd.read_csv("./test_data/%s"%test_mode)

    # Check missing start_week == end_week
    try:
        assert sum(df["timestamp"] != df["end_week"]) == 0
    except AssertionError as exc:
        raise ValueError(
            "end_week is not always the same as start_week, check the raw file"
        ) from exc

    try:
        df = df.astype(type_dict)
    except KeyError as exc:
        raise ValueError("Expected column(s) missed, The dataset "
                         "schema may have changed. Please investigate and "
                         "amend the code.") from exc

    df = df[df["state"] != "United States"]
    df.loc[df["state"] == "New York City", "state"] = "New York"

    # Add population info
    keep_columns.extend(["timestamp", "geo_id", "population"])
    df = df.merge(map_df, on="state")[keep_columns]

    return df
