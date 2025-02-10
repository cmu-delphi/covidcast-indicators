# -*- coding: utf-8 -*-
"""Functions for pulling NSSP ER data."""
import logging
import random
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError

import pandas as pd
from delphi_utils import create_backup_csv
from sodapy import Socrata

from .constants import MAIN_DATASET_ID, PRELIM_DATASET_ID, PRELIM_SIGNALS_MAP, PRELIM_TYPE_DICT, SIGNALS_MAP, TYPE_DICT


def check_last_updated(socrata_token, dataset_id, logger):
    """
    Check last updated timestamp to determine data should be pulled or not.

    Note -- the behavior of the api fail is to treat is as stale
    as having possible duplicate is preferable compared to possible missing data

    Parameters
    ----------
    socrata_token
    dataset_id
    logger

    Returns
    -------

    """
    recently_updated_source = True
    try:
        client = Socrata("data.cdc.gov", socrata_token)
        response = client.get_metadata(dataset_id)

        updated_timestamp = datetime.utcfromtimestamp(int(response["rowsUpdatedAt"]))
        now = datetime.utcnow()
        recently_updated_source = (now - updated_timestamp) < timedelta(days=1)

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
    logger.info("Pulling data from Socrata API")
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
        create_backup_csv(df, backup_dir, False, logger=logger)
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
    logger: Optional[logging.Logger]
        logger object

    Returns
    -------
    pd.DataFrame
        Dataframe as described above.
    """
    # Pull data from Socrata API
    df = (
        pull_data(socrata_token, MAIN_DATASET_ID, backup_dir, logger)
        if not custom_run
        else pull_data_from_file(backup_dir, issue_date, logger, prelim_flag=False)
    )

    recently_updated = True if custom_run else check_last_updated(socrata_token, MAIN_DATASET_ID, logger)

    keep_columns = list(TYPE_DICT.keys())

    if not df.empty and recently_updated:
        df = df.rename(columns={"weekendingdate": "timestamp", "jurisdiction": "geo_id"})
        filtered_type_dict = TYPE_DICT.copy(deep=True)

        for signal, col_name in SIGNALS_MAP.items():
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


def pull_preliminary_nhsn_data(
    socrata_token: str,
    backup_dir: str,
    custom_run: bool,
    issue_date: Optional[str],
    logger: Optional[logging.Logger] = None,
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
    df = (
        pull_data(socrata_token, PRELIM_DATASET_ID, backup_dir, logger)
        if not custom_run
        else pull_data_from_file(backup_dir, issue_date, logger, prelim_flag=True)
    )

    keep_columns = list(PRELIM_TYPE_DICT.keys())
    recently_updated = True if custom_run else check_last_updated(socrata_token, PRELIM_DATASET_ID, logger)

    if not df.empty and recently_updated:
        df = df.rename(columns={"weekendingdate": "timestamp", "jurisdiction": "geo_id"})
        filtered_type_dict = PRELIM_TYPE_DICT.copy(deep=True)

        for signal, col_name in PRELIM_SIGNALS_MAP.items():
            try:
                df[signal] = df[col_name]
            except KeyError:
                logger.info("column not available in data", col_name=col_name, signal=signal)
                keep_columns.remove(signal)
                del filtered_type_dict[signal]

        df = df[keep_columns]
        df = df.astype(filtered_type_dict)

        df["geo_id"] = df["geo_id"].str.lower()
        df.loc[df["geo_id"] == "usa", "geo_id"] = "us"
    else:
        df = pd.DataFrame(columns=keep_columns)

    return df
