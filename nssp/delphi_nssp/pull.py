# -*- coding: utf-8 -*-
"""Functions for pulling NSSP ER data."""

import functools
import logging
import sys
import textwrap
from os import makedirs, path
from typing import Optional

import pandas as pd
import paramiko
from delphi_utils import create_backup_csv
from sodapy import Socrata

from .constants import DATASET_ID, NEWLINE, SIGNALS, SIGNALS_MAP, TYPE_DICT


def print_callback(remote_file_name, logger, bytes_so_far, bytes_total, progress_chunks):
    """Print the callback information."""
    rough_percent_transferred = int(100 * (bytes_so_far / bytes_total))
    if rough_percent_transferred in progress_chunks:
        logger.info("Transfer in progress", remote_file_name=remote_file_name, percent=rough_percent_transferred)
        # Remove progress chunk, so it is not logged again
        progress_chunks.remove(rough_percent_transferred)


def get_source_data(params, logger):
    """
    Download historical source data from a backup server.

    This function is typically used in patching only. Normal runs grab latest data from SODA API.

    This function uses "user" configuration under "patch" section in params.json to specify
    a username with local key-based access to connect to server where backup nssp source data is stored.
    It uses "backup_dir" config under "common" section to locate backup files on remote server.
    It then searches for CSV files that match the inclusive range of issue dates
    specified by 'start_issue', and 'end_issue' config.

    These CSV files are then downloaded and stored in the local 'source_dir' directory.
    """
    makedirs(params["patch"]["source_dir"], exist_ok=True)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    host = params["patch"]["source_host"]
    user = params["patch"]["user"]
    ssh.connect(host, username=user)

    # Generate file names of source files to download
    dates = pd.date_range(start=params["patch"]["start_issue"], end=params["patch"]["end_issue"])
    remote_source_files = [f"{date.strftime('%Y%m%d')}.csv.gz" for date in dates]

    # Download source files
    sftp = ssh.open_sftp()
    try:
        sftp.stat(params["common"]["backup_dir"])
    except IOError:
        logger.error("Source backup directory does not exist on the remote server.")

    sftp.chdir(params["common"]["backup_dir"])

    num_files_transferred = 0
    for remote_file_name in remote_source_files:
        callback_for_filename = functools.partial(print_callback, remote_file_name, logger, progress_chunks=[0, 50])
        local_file_path = path.join(params["patch"]["source_dir"], remote_file_name)
        try:
            sftp.stat(remote_file_name)
        except IOError:
            logger.warning(
                "Source backup for this date does not exist on the remote server.",
                missing_filename=remote_file_name,
            )
            continue
        sftp.get(remote_file_name, local_file_path, callback=callback_for_filename)
        logger.info("Transfer finished", remote_file_name=remote_file_name, local_file_path=local_file_path)
        num_files_transferred += 1
    ssh.close()

    if num_files_transferred == 0:
        logger.error("No source data was transferred. Check the source backup server for potential issues.")
        sys.exit(1)

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


def pull_nssp_data(
    socrata_token: str,
    backup_dir: str,
    custom_run: bool,
    issue_date: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
):
    """Pull the NSSP ER visits primary dataset.

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
    if not custom_run:
        socrata_results = pull_with_socrata_api(socrata_token, DATASET_ID)
        df_ervisits = pd.DataFrame.from_records(socrata_results)
        create_backup_csv(df_ervisits, backup_dir, custom_run, logger=logger)
        logger.info("Number of records grabbed", num_records=len(df_ervisits), source="Socrata API")
    elif custom_run and logger.name == "delphi_nssp.patch":
        if issue_date is None:
            raise ValueError("Issue date is required for patching")
        source_filename = f"{backup_dir}/{issue_date}.csv.gz"
        if not path.isfile(source_filename):
            logger.warning("No primary source data found", source=source_filename, issue_date=issue_date)
            return None
        df_ervisits = pd.read_csv(source_filename)
        logger.info(
            "Number of records grabbed",
            num_records=len(df_ervisits),
            source=source_filename,
        )

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
