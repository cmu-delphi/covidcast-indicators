# -*- coding: utf-8 -*-
"""Functions for pulling NSSP ER data."""
import logging
import textwrap
from typing import Optional

import pandas as pd
from delphi_utils import create_backup_csv
from sodapy import Socrata

from .constants import SIGNALS_MAP, TYPE_DICT, PARTIAL_SIGNALS


def process_signal_data(df):
    for signal, signal_parts in SIGNALS_MAP.items():
        df[signal] = sum([df[col] for col in signal_parts])
    return df


def pull_nhsn_data(socrata_token: str, backup_dir: str, custom_run: bool, logger: Optional[logging.Logger] = None):
    """Pull the latest NSSP ER visits data, and conforms it into a dataset.

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
    client = Socrata("data.cdc.gov", socrata_token)
    results = []
    offset = 0
    limit = 50000  # maximum limit allowed by SODA 2.0
    while True:
        page = client.get("ua7e-t2fy", limit=limit, offset=offset)
        if not page:
            break  # exit the loop if no more results
        results.extend(page)
        offset += limit
    df = pd.DataFrame.from_records(results)

    create_backup_csv(df, backup_dir, custom_run, logger=logger)

    df = df.rename(columns={"weekendingdate": "timestamp"})
    df = df[TYPE_DICT.keys()]
    df = df.astype(TYPE_DICT)
    processed_df = process_signal_data(df)
    processed_df = processed_df.drop(columns=PARTIAL_SIGNALS)
    return processed_df
