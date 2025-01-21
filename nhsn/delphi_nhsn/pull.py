# -*- coding: utf-8 -*-
"""Functions for pulling NSSP ER data."""
import logging
from typing import Optional
from datetime import datetime

import pandas as pd
from delphi_utils import create_backup_csv
from sodapy import Socrata

from .constants import PRELIM_TYPE_DICT, TYPE_DICT

def check_update(socrata_token: str, dataset_id: str):
    client = Socrata("data.cdc.gov", socrata_token)
    response = client.get_metadata(dataset_id)
    updated_timestamp = datetime.utcfromtimestamp(int(response["rowsUpdatedAt"]))
    print("bob")



def pull_data(socrata_token: str, dataset_id: str):
    """Pull data from Socrata API."""
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

    df = pd.DataFrame.from_records(results)
    return df


def pull_nhsn_data(socrata_token: str, backup_dir: str, custom_run: bool, logger: Optional[logging.Logger] = None):
    """Pull the latest NHSN hospital admission data, and conforms it into a dataset.

    The output dataset has:

    - Each row corresponds to a single observation
    - Each row additionally has columns for the signals in SIGNALS

    Parameters
    ----------
    socrata_token: str
        My App Token for pulling the NHSN data
    backup_dir: str
        Directory to which to save raw backup data
    custom_run: bool
        Flag indicating if the current run is a patch. If so, don't save any data to disk
    logger: Optional[logging.Logger]
        logger object

    Returns
    -------
    pd.DataFrame
        Dataframe as described above.
    """
    # Pull data from Socrata API
    check_update(socrata_token, "ua7e-t2fy")
    df = pull_data(socrata_token, dataset_id="ua7e-t2fy")

    keep_columns = list(TYPE_DICT.keys())

    if not df.empty:
        create_backup_csv(df, backup_dir, custom_run, logger=logger)
        df = df.rename(columns={"weekendingdate": "timestamp", "jurisdiction": "geo_id"})
        df = df[keep_columns]
        df["geo_id"] = df["geo_id"].str.lower()
        df.loc[df["geo_id"] == "usa", "geo_id"] = "us"
        df = df.astype(TYPE_DICT)
    else:
        df = pd.DataFrame(columns=keep_columns)

    return df


def pull_preliminary_nhsn_data(
    socrata_token: str, backup_dir: str, custom_run: bool, logger: Optional[logging.Logger] = None
):
    """Pull the latest preliminary NHSN hospital admission data, and conforms it into a dataset.

    The output dataset has:

    - Each row corresponds to a single observation
    - Each row additionally has columns for the signals in SIGNALS

    Parameters
    ----------
    socrata_token: str
        My App Token for pulling the NHSN data
    backup_dir: str
        Directory to which to save raw backup data
    custom_run: bool
        Flag indicating if the current run is a patch. If so, don't save any data to disk
    logger: Optional[logging.Logger]
        logger object

    Returns
    -------
    pd.DataFrame
        Dataframe as described above.
    """
    # Pull data from Socrata API
    check_update(socrata_token, "mpgq-jmmr")
    df = pull_data(socrata_token, dataset_id="mpgq-jmmr")

    keep_columns = list(PRELIM_TYPE_DICT.keys())

    if not df.empty:
        create_backup_csv(df, backup_dir, custom_run, sensor="prelim", logger=logger)
        df = df.rename(columns={"weekendingdate": "timestamp", "jurisdiction": "geo_id"})
        df = df[keep_columns]
        df = df.astype(PRELIM_TYPE_DICT)
        df["geo_id"] = df["geo_id"].str.lower()
        df.loc[df["geo_id"] == "usa", "geo_id"] = "us"
    else:
        df = pd.DataFrame(columns=keep_columns)

    return df
