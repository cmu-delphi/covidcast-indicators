# -*- coding: utf-8 -*-
"""Functions for pulling NCHS mortality data API."""

import numpy as np
import pandas as pd
from sodapy import Socrata

from .constants import (
    METRICS,
    NEWLINE,
)


def construct_typedicts():
    """Create the type conversion dictionary for dataframe."""
    # basic type conversion
    type_dict = {key: float for key in METRICS}
    type_dict["timestamp"] = "datetime64[ns]"
    type_dict["geography"] = str 
    type_dict["county"] = str
    type_dict["fips"] = int
    return type_dict


def warn_string(df, type_dict):
    """Format the warning string."""
    return f"""
Expected column(s) missed, The dataset schema may
have changed. Please investigate and amend the code.

Columns needed:
{NEWLINE.join(sorted(type_dict.keys()))}

Columns available:
{NEWLINE.join(sorted(df.columns))}
"""


def pull_nssp_data(socrata_token: str):
    """Pull the latest NWSS Wastewater data, and conforms it into a dataset.

    The output dataset has:

    - Each row corresponds to a single observation
    - Each row additionally has columns for the signals in METRICS

    Parameters
    ----------
    socrata_token: str
        My App Token for pulling the NWSS data (could be the same as the nchs data)
    test_file: Optional[str]
        When not null, name of file from which to read test data

    Returns
    -------
    pd.DataFrame
        Dataframe as described above.
    """
    type_dict = construct_typedicts()

    # Pull data from Socrata API
    client = Socrata("data.cdc.gov", socrata_token)
    results = []
    offset = 0
    limit = 50000  # maximum limit allowed by SODA 2.0
    while True:
        page = client.get("rdmq-nq56", limit=limit, offset=offset)
        if not page:
            break  # exit the loop if no more results
        results.extend(page)
        offset += limit
    df_ervisits = pd.DataFrame.from_records(results)
    df_ervisits = df_ervisits.rename(columns={"week_end": "timestamp", 
                                              "percent_visits_smoothed":"percent_visits_smoothed_combined",
                                              "percent_visits_smoothed_1":"percent_visits_smoothed_influenza",})

    try:
        df_ervisits = df_ervisits.astype(type_dict)
    except KeyError as exc:
        raise ValueError(warn_string(df_ervisits, type_dict)) from exc

    keep_columns = ["timestamp", "geography", "county", "fips"]
    return df_ervisits[METRICS + keep_columns]
