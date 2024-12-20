# -*- coding: utf-8 -*-
"""Functions for pulling NSSP ER data."""
import logging
import textwrap
from typing import Optional

import pandas as pd
from delphi_utils import create_backup_csv
from sodapy import Socrata

from .constants import (
    NEWLINE,
    SECONDARY_COLS_MAP,
    SECONDARY_KEEP_COLS,
    SECONDARY_SIGNALS_MAP,
    SECONDARY_TYPE_DICT,
    SIGNALS,
    SIGNALS_MAP,
    TYPE_DICT,
)


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


def pull_with_socrata_api(socrata_token: str, dataset_id: str):
    """Pull data from Socrata API.

    Parameters
    ----------
    socrata_token: str
        My App Token for pulling the NSSP data (could be the same as the nchs data)
    dataset_id: str
        The dataset id to pull data from

    Returns
    -------
    list of dictionaries, each representing a row in the dataset
    """
    client = Socrata("data.cdc.gov", socrata_token)
    results = []
    offset = 0
    limit = 50000  # maximum limit allowed by SODA 2.0
    while True:
        page = client.get(dataset_id, limit=limit, offset=offset)
        if not page:
            break  # exit the loop if no more results
        results.extend(page)
        offset += limit
    return results


def pull_nssp_data(socrata_token: str, backup_dir: str, custom_run: bool, logger: Optional[logging.Logger] = None):
    """Pull the latest NSSP ER visits primary dataset.

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
    socrata_results = pull_with_socrata_api(socrata_token, "rdmq-nq56")
    df_ervisits = pd.DataFrame.from_records(socrata_results)
    create_backup_csv(df_ervisits, backup_dir, custom_run, logger=logger)
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


def secondary_pull_nssp_data(
    socrata_token: str, backup_dir: str, custom_run: bool, logger: Optional[logging.Logger] = None
):
    """Pull the latest NSSP ER visits secondary dataset.

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
    socrata_results = pull_with_socrata_api(socrata_token, "7mra-9cq9")
    df_ervisits = pd.DataFrame.from_records(socrata_results)
    create_backup_csv(df_ervisits, backup_dir, custom_run, sensor="secondary", logger=logger)
    df_ervisits = df_ervisits.rename(columns=SECONDARY_COLS_MAP)

    # geo_type is not provided in the dataset, so we infer it from the geo_value
    # which is either state names, "National" or hhs region numbers
    df_ervisits["geo_type"] = "state"

    df_ervisits.loc[df_ervisits["geo_value"] == "National", "geo_type"] = "nation"

    hhs_region_mask = df_ervisits["geo_value"].str.lower().str.startswith("region ")
    df_ervisits.loc[hhs_region_mask, "geo_value"] = df_ervisits.loc[hhs_region_mask, "geo_value"].str.replace(
        "Region ", ""
    )
    df_ervisits.loc[hhs_region_mask, "geo_type"] = "hhs"

    df_ervisits["signal"] = df_ervisits["signal"].map(SECONDARY_SIGNALS_MAP)

    df_ervisits = df_ervisits[SECONDARY_KEEP_COLS]

    try:
        df_ervisits = df_ervisits.astype(SECONDARY_TYPE_DICT)
    except KeyError as exc:
        raise ValueError(warn_string(df_ervisits, SECONDARY_TYPE_DICT)) from exc

    return df_ervisits
