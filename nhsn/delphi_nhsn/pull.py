# -*- coding: utf-8 -*-
"""Functions for pulling NSSP ER data."""
import logging
from typing import Optional

import pandas as pd
from delphi_utils import create_backup_csv
from sodapy import Socrata

from .constants import (
    MAIN_DATASET_ID,
    PRELIM_SIGNALS_MAP,
    PRELIM_TYPE_DICT,
    PRELIMINARY_DATASET_ID,
    SIGNALS_MAP,
    TYPE_DICT,
)


def pull_data(socrata_token: str, dataset_id: str, logger):
    """Pull data from Socrata API."""
    client = Socrata("data.cdc.gov", socrata_token)
    results = []
    offset = 0
    limit = 50000  # maximum limit allowed by SODA 2.0
    logging.info("Pulling data from Socrata API")
    while True:
        page = client.get(dataset_id, limit=limit, offset=offset)
        if not page:
            break  # exit the loop if no more results
        results.extend(page)
        offset += limit

    df = pd.DataFrame.from_records(results)
    logger.info("Complete pulling data", num_rows=len(results))
    return df


def process_data(df, dataset_id) -> pd.DataFrame:
    """
    Filter unnecessary columns and process data to generate signal values.

    Parameters
    ----------
    df: source data
    dataset_id: string to determine dataset origin

    Returns
    -------
    pd.DataFrame
        Dataframe as described above.
    """
    signal_map = dict()
    keep_columns = []
    type_dict = dict()
    if dataset_id == MAIN_DATASET_ID:
        signal_map = SIGNALS_MAP
        keep_columns = list(TYPE_DICT.keys())
        type_dict = TYPE_DICT
    elif dataset_id == PRELIMINARY_DATASET_ID:
        signal_map = PRELIM_SIGNALS_MAP
        keep_columns = list(PRELIM_TYPE_DICT.keys())
        type_dict = PRELIM_TYPE_DICT

    df = df.rename(columns={"weekendingdate": "timestamp", "jurisdiction": "geo_id"})

    for signal, col in signal_map.items():
        # if signal requires additional processing eg) getting proportions
        if isinstance(col) is list:
            if "prop" in signal:
                df[signal] = df[col[0]].astype(float) / df[col[1]].astype(float)
        else:
            df[signal] = df[col]

    df = df[keep_columns]
    df["geo_id"] = df["geo_id"].str.lower()
    df.loc[df["geo_id"] == "usa", "geo_id"] = "us"
    df = df.astype(type_dict)
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
    logger.info("Pulling main dataset")
    source_df = pull_data(socrata_token, dataset_id=MAIN_DATASET_ID, logger=logger)

    keep_columns = list(TYPE_DICT.keys())
    df = pd.DataFrame(columns=keep_columns)
    if not source_df.empty:
        create_backup_csv(source_df, backup_dir, custom_run, logger=logger)
        df = process_data(source_df, dataset_id=MAIN_DATASET_ID)

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
    logger.info("Pulling preliminary dataset")
    source_df = pull_data(socrata_token, dataset_id=PRELIMINARY_DATASET_ID, logger=logger)

    keep_columns = list(PRELIM_TYPE_DICT.keys())
    df = pd.DataFrame(columns=keep_columns)

    if not source_df.empty:
        create_backup_csv(source_df, backup_dir, custom_run, sensor="prelim", logger=logger)
        df = process_data(source_df, dataset_id=PRELIMINARY_DATASET_ID)

    return df
