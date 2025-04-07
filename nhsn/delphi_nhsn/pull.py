# -*- coding: utf-8 -*-
"""Functions for pulling NSSP ER data."""
import copy
import logging
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError

import pandas as pd
from delphi_utils import create_backup_csv
from sodapy import Socrata

from .constants import (
    MAIN_DATASET_ID,
    PRELIM_DATASET_ID,
    PRELIM_SIGNALS_MAP,
    PRELIM_TYPE_DICT,
    RECENTLY_UPDATED_DIFF,
    SIGNALS_MAP,
    TYPE_DICT,
)


def check_last_updated(socrata_token, dataset_id, logger):
    """
    Check last updated timestamp to determine if data should be pulled or not.

    Note -- if the call to the API fails, the behavior is to treat the data as stale,
    as possibly having duplicate is preferable to missing data

    Parameters
    ----------
    socrata_token
    dataset_id
    logger

    Returns bool
    -------

    """
    recently_updated_source = True
    try:
        client = Socrata("data.cdc.gov", socrata_token)
        response = client.get_metadata(dataset_id)

        updated_timestamp = datetime.utcfromtimestamp(int(response["rowsUpdatedAt"]))
        now = datetime.utcnow()
        # currently set to run twice a week, RECENTLY_UPDATED_DIFF may need adjusting based on the cadence
        recently_updated_source = (now - updated_timestamp) < RECENTLY_UPDATED_DIFF

        prelim_prefix = "Preliminary " if dataset_id == PRELIM_DATASET_ID else ""
        if recently_updated_source:
            logger.info(
                f"{prelim_prefix}NHSN data was recently updated; Pulling data", updated_timestamp=updated_timestamp
            )
        else:
            logger.info(f"{prelim_prefix}NHSN data is stale; Skipping", updated_timestamp=updated_timestamp)
    # pylint: disable=W0703
    except Exception as e:
        logger.info("error while processing socrata metadata; treating data as stale", error=str(e))
    return recently_updated_source


def pull_data(socrata_token: str, dataset_id: str, backup_dir: str, logger):
    """Pull data from Socrata API."""
    client = Socrata("data.cdc.gov", socrata_token)
    logger.info(
        f"Pulling {'main' if dataset_id == MAIN_DATASET_ID else 'preliminary'} data from Socrata API",
        dataset_id=dataset_id,
    )
    results = []
    offset = 0
    limit = 50000  # maximum limit allowed by SODA 2.0
    # retry logic for 500 error
    try:
        page = client.get(dataset_id, limit=limit, offset=offset)
    except HTTPError as err:
        if err.code == 503:
            time.sleep(2 + random.randint(0, 1000) / 1000.0)
            page = client.get(dataset_id, limit=limit, offset=offset)
        else:
            logger.info("Error pulling data from Socrata API", error=str(err))
            raise err

    while len(page) > 0:
        results.extend(page)
        offset += limit
        page = client.get(dataset_id, limit=limit, offset=offset)

    if results:
        df = pd.DataFrame.from_records(results)
        sensor = "prelim" if dataset_id == PRELIM_DATASET_ID else None
        create_backup_csv(df, backup_dir, False, sensor=sensor, logger=logger)
    else:
        df = pd.DataFrame()
    return df


def pull_data_from_file(filepath: str, issue_date: str, logger, prelim_flag=False) -> pd.DataFrame:
    """
    Pull data from source file.

    The source file is generated from delphi_utils.create_backup_csv
    Parameters
    ----------
    filepath: full path where the source file is located
    issue_date: date when the file was pulled / generated
    logger
    prelim_flag: boolean to indicate which dataset to grab

    Returns
    -------
    pd.DataFrame
        Dataframe as described above.
    """
    df = pd.DataFrame()
    if issue_date:
        issue_date = issue_date.replace("-", "")
        filename = f"{issue_date}_prelim.csv.gz" if prelim_flag else f"{issue_date}.csv.gz"
        backup_file = Path(filepath, filename)

        if backup_file.exists():
            df = pd.read_csv(backup_file, compression="gzip")
            logger.info("Pulling data from file", file=filename, num_rows=len(df))
    return df


def pull_nhsn_data(
    socrata_token: str,
    backup_dir: str,
    custom_run: bool,
    issue_date: Optional[str],
    preliminary: bool = False,
    logger: Optional[logging.Logger] = None,
):
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
    preliminary: bool
        Flag indicating if the grabbing main or preliminary data
    issue_date:
        date to indicate which backup file to pull for patching
    logger: Optional[logging.Logger]
        logger object

    Returns
    -------
    pd.DataFrame
        Dataframe as described above.
    """
    dataset_id = PRELIM_DATASET_ID if preliminary else MAIN_DATASET_ID
    # Pull data from Socrata API
    df = (
        pull_data(socrata_token, dataset_id, backup_dir, logger)
        if not custom_run
        else pull_data_from_file(backup_dir, issue_date, logger, prelim_flag=preliminary)
    )

    recently_updated = True if custom_run else check_last_updated(socrata_token, dataset_id, logger)

    type_dict = PRELIM_TYPE_DICT if preliminary else TYPE_DICT
    keep_columns = list(type_dict.keys())
    filtered_type_dict = copy.deepcopy(type_dict)

    signal_map = PRELIM_SIGNALS_MAP if preliminary else SIGNALS_MAP

    if not df.empty and recently_updated:
        df = df.rename(columns={"weekendingdate": "timestamp", "jurisdiction": "geo_id"})

        for signal, col_name in signal_map.items():
            # older backups don't have certain columns
            try:
                df[signal] = df[col_name]
            except KeyError:
                logger.info("column not available in data", col_name=col_name)
                keep_columns.remove(signal)
                del filtered_type_dict[signal]

        df = df[keep_columns]
        df["geo_id"] = df["geo_id"].str.lower()
        df.loc[df["geo_id"] == "usa", "geo_id"] = "us"

        df = df.astype(filtered_type_dict)
    else:
        df = pd.DataFrame(columns=keep_columns)

    return df
