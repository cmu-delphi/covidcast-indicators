# -*- coding: utf-8 -*-
"""Functions for pulling NSSP ER data."""

import textwrap

import pandas as pd
from sodapy import Socrata

from .constants import NEWLINE, SIGNALS, SIGNALS_MAP, TYPE_DICT


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


def pull_nssp_data(socrata_token: str, issue_date: str = None, source_dir: str = None) -> pd.DataFrame:
    """Pull the latest NSSP ER visits data, and conforms it into a dataset.

    The output dataset has:

    - Each row corresponds to a single observation
    - Each row additionally has columns for the signals in SIGNALS

    Parameters
    ----------
    socrata_token: str
        My App Token for pulling the NWSS data (could be the same as the nchs data)
    issue_date: Optional[str]
        (patching mode only) YYYY-MM-DD formatted date of the issue to pull data for.
    source_dir: Optional[str]
        (patching mode only) Path to the directory containing the source data csv files. 
        The files in source_dir are expected to be named yyyy-mm-dd.csv
    test_file: Optional[str]
        When not null, name of file from which to read test data

    Returns
    -------
    pd.DataFrame
        Dataframe as described above.
    """
    if not issue_date:
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
    else:
        df_ervisits = pd.read_csv(f"{source_dir}/{issue_date}.csv")
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
