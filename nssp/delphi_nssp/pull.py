# -*- coding: utf-8 -*-
"""Functions for pulling NSSP ER data."""

import textwrap

import pandas as pd
from sodapy import Socrata

from .constants import *


def warn_string(df, type_dict):
    """Format the warning string."""
    warn = textwrap.dedent(
        f"""
        Expected column(s) missed, The dataset schema may
        have changed. Please investigate and amend the code.

        Columns needed:
        {NEWLINE.join(sorted(type_dict.keys()))}

        Columns available:
        {NEWLINE.join(sorted(df.columns))}
    """
    )

    return warn


def pull_nssp_data(socrata_token: str):
    """Pull the latest NSSP ER visits primary dataset
    https://data.cdc.gov/Public-Health-Surveillance/NSSP-Emergency-Department-Visit-Trajectories-by-St/rdmq-nq56/data_preview

    Parameters
    ----------
    socrata_token: str
        My App Token for pulling the NSSP data (could be the same as the nchs data)

    Returns
    -------
    pd.DataFrame
        Dataframe as described above.
    """
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
    df_ervisits = df_ervisits.rename(columns={"week_end": "timestamp"})
    df_ervisits = df_ervisits.rename(columns=SIGNALS_MAP)

    try:
        df_ervisits = df_ervisits.astype(TYPE_DICT)
    except KeyError as exc:
        raise ValueError(warn_string(df_ervisits, TYPE_DICT)) from exc

    # Format county fips to all be 5 digits with leading zeros
    df_ervisits["fips"] = df_ervisits["fips"].apply(lambda x: str(x).zfill(5) if str(x) != "0" else "0")

    keep_columns = ["timestamp", "geography", "county", "fips"]
    return df_ervisits[SIGNALS + keep_columns]


def secondary_pull_nssp_data(socrata_token: str):
    """Pull the latest NSSP ER visits secondary dataset:
    https://data.cdc.gov/Public-Health-Surveillance/2023-Respiratory-Virus-Response-NSSP-Emergency-Dep/7mra-9cq9/data_preview

    The output dataset has:

    - Each row corresponds to a single observation

    Parameters
    ----------
    socrata_token: str
        My App Token for pulling the NSSP data (could be the same as the nchs data)

    Returns
    -------
    pd.DataFrame
        Dataframe as described above.
    """
    # Pull data from Socrata API
    client = Socrata("data.cdc.gov", socrata_token)
    results = []
    offset = 0
    limit = 50000  # maximum limit allowed by SODA 2.0
    while True:
        page = client.get("7mra-9cq9", limit=limit, offset=offset)
        if not page:
            break  # exit the loop if no more results
        results.extend(page)
        offset += limit
    df_ervisits = pd.DataFrame.from_records(results)
    df_ervisits = df_ervisits.rename(columns=SECONDARY_COLS_MAP)

    # geo_type is not provided in the dataset, so we infer it from the geo_value
    # which is either state names, "National" or hhs region numbers
    df_ervisits['geo_type'] = 'state'

    df_ervisits.loc[df_ervisits['geo_value'] == 'National', 'geo_type'] = 'nation'

    hhs_region_mask = df_ervisits['geo_value'].str.startswith('Region ')
    df_ervisits.loc[hhs_region_mask, 'geo_value'] = df_ervisits.loc[hhs_region_mask, 'geo_value'].str.replace('Region ', '')
    df_ervisits.loc[hhs_region_mask, 'geo_type'] = 'hhs'

    df_ervisits['signal'] = df_ervisits['signal'].map(SECONDARY_SIGNALS_MAP)

    df_ervisits = df_ervisits[SECONDARY_KEEP_COLS]

    try:
        df_ervisits = df_ervisits.astype(SECONDARY_TYPE_DICT)
    except KeyError as exc:
        raise ValueError(warn_string(df_ervisits, SECONDARY_TYPE_DICT)) from exc

    return df_ervisits
