# -*- coding: utf-8 -*-
"""Functions for pulling the public NSSP ED respiratory feed.

The feed is a public Socrata dataset, so no app token is required; an optional
token may still be supplied to raise the API rate limit. Pagination follows
the same pattern as the other Socrata-based indicators in this repo
(e.g. ``nssp``, ``nwss_wastewater``).
"""

import textwrap
from logging import Logger
from os import makedirs
from typing import List, Optional

import pandas as pd
from delphi_utils import create_backup_csv
from sodapy import Socrata

from .constants import (
    DATASET_DOMAIN,
    DATASET_ID,
    EXPECTED_COLUMNS,
    MAX_PLAUSIBLE_PCT,
    MIN_PLAUSIBLE_PCT,
    NATIONAL_LABEL,
    NEWLINE,
    PATHOGENS,
    SIGNALS_BASE,
    SIGNALS_MAP,
    TYPE_DICT,
)


def warn_string(df: pd.DataFrame, expected_columns) -> str:
    """Format the warning string for an unexpected feed schema."""
    return textwrap.dedent(
        f"""\
        Expected column(s) missed. The dataset schema may
        have changed. Please investigate and amend the code.

        Columns needed:
        {NEWLINE.join(sorted(expected_columns))}

        Columns available:
        {NEWLINE.join(sorted(df.columns))}
        """
    )


def pull_with_socrata_api(
    dataset_id: str,
    socrata_token: Optional[str] = None,
    domain: str = DATASET_DOMAIN,
) -> List[dict]:
    """Pull every row of a Socrata dataset, paginating through the API.

    Parameters
    ----------
    dataset_id: str
        The Socrata dataset id to pull.
    socrata_token: Optional[str]
        Optional app token. The NSSP ED respiratory feed is public, so this
        may be empty/None; a token only raises the rate limit.
    domain: str
        The Socrata domain hosting the dataset.

    Returns
    -------
    list of dictionaries, each representing a row in the dataset
    """
    # sodapy accepts a None app token for anonymous access to public datasets.
    client = Socrata(domain, socrata_token or None, timeout=50)
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


def validate_schema(df: pd.DataFrame) -> None:
    """Fail loudly if the feed schema changed.

    Raises
    ------
    ValueError
        If any expected column is missing from the pulled dataframe.
    """
    missing = EXPECTED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(warn_string(df, EXPECTED_COLUMNS))


def drop_implausible_values(df: pd.DataFrame, logger: Optional[Logger] = None) -> pd.DataFrame:
    """Drop rows whose percentage is outside the plausible [0, 100] range.

    Percentages of ED visits cannot be negative or exceed 100; such rows are
    bad source data. They are dropped (not imputed) and counted in the logs
    so a broken feed is visible without killing the whole run.
    """
    df = df.copy()
    df["percent_visits"] = pd.to_numeric(df["percent_visits"], errors="coerce")
    bad_mask = df["percent_visits"].isna() | (df["percent_visits"] < MIN_PLAUSIBLE_PCT) | (
        df["percent_visits"] > MAX_PLAUSIBLE_PCT
    )
    n_bad = int(bad_mask.sum())
    if n_bad and logger is not None:
        logger.error(
            "Dropping rows with implausible percent_visits",
            num_dropped=n_bad,
            min_allowed=MIN_PLAUSIBLE_PCT,
            max_allowed=MAX_PLAUSIBLE_PCT,
        )
    return df.loc[~bad_mask].reset_index(drop=True)


def pivot_signals(df: pd.DataFrame, logger: Optional[Logger] = None) -> pd.DataFrame:
    """Pivot the long feed into one column per signal.

    Unknown pathogen labels raise instead of being silently dropped, so a feed
    change (e.g. a new pathogen) is caught at pull time.
    """
    unknown = set(df["pathogen"].unique()) - set(PATHOGENS)
    if unknown:
        raise ValueError(
            f"Unknown pathogen label(s) in feed: {sorted(unknown)}. "
            f"Known labels: {PATHOGENS}. Update SIGNALS_MAP if the feed added a pathogen."
        )
    # The feed should have at most one row per (day, geography, pathogen).
    # If the source ever duplicates rows, average them and say so loudly.
    dupes = df.duplicated(subset=["timestamp", "geography", "pathogen"]).sum()
    if dupes and logger is not None:
        logger.warning(
            "Duplicate (date, geography, pathogen) rows in feed; averaging",
            num_duplicates=int(dupes),
        )
    pivoted = df.pivot_table(
        index=["timestamp", "geography"],
        columns="pathogen",
        values="percent_visits",
        aggfunc="mean",
    ).reset_index()
    pivoted = pivoted.rename(columns=SIGNALS_MAP)
    # Guarantee every expected signal column exists even if a pathogen is
    # temporarily absent from the feed (columns stay NaN and are filtered
    # downstream).
    for signal in SIGNALS_BASE:
        if signal not in pivoted.columns:
            pivoted[signal] = float("nan")
    return pivoted[["timestamp", "geography"] + SIGNALS_BASE]


def pull_nssp_ed_data(
    backup_dir: str,
    custom_run: bool,
    socrata_token: Optional[str] = None,
    source_df: Optional[pd.DataFrame] = None,
    logger: Optional[Logger] = None,
) -> pd.DataFrame:
    """Pull the NSSP ED respiratory feed and conform it into a dataset.

    The output dataframe has one row per (day, geography) with columns
    ``timestamp``, ``geography``, and one ``pct_ed_visits_*`` column per
    pathogen.

    Parameters
    ----------
    backup_dir: str
        Directory where the raw pulled data is archived.
    custom_run: bool
        If True, skip the live pull and the raw backup; the data must be
        supplied via ``source_df`` (used by patching).
    socrata_token: Optional[str]
        Optional Socrata app token (public feed; only raises rate limits).
    source_df: Optional[pd.DataFrame]
        Pre-pulled raw feed dataframe, already in the feed's long format.
        Required when ``custom_run`` is True.
    logger: Optional[Logger]
        Structured logger.

    Returns
    -------
    pd.DataFrame
        Conformed dataframe as described above.
    """
    if custom_run:
        if source_df is None:
            raise ValueError("source_df is required for custom (patch) runs")
        df_feed = source_df.copy()
        if logger is not None:
            logger.info("Using supplied source data, skipping live pull", num_records=len(df_feed))
    else:
        makedirs(backup_dir, exist_ok=True)
        socrata_results = pull_with_socrata_api(DATASET_ID, socrata_token=socrata_token)
        df_feed = pd.DataFrame.from_records(socrata_results)
        create_backup_csv(df_feed, backup_dir, custom_run, logger=logger)
        if logger is not None:
            logger.info("Number of records grabbed", num_records=len(df_feed), source="Socrata API")

    validate_schema(df_feed)

    df_feed = df_feed.rename(columns={"date": "timestamp"})
    try:
        df_feed = df_feed.astype(TYPE_DICT)
    except (KeyError, ValueError, TypeError) as exc:
        raise ValueError(warn_string(df_feed, EXPECTED_COLUMNS)) from exc

    df_feed = drop_implausible_values(df_feed, logger=logger)
    df_signals = pivot_signals(df_feed, logger=logger)
    return df_signals.sort_values(["geography", "timestamp"]).reset_index(drop=True)


def national_label() -> str:
    """Return the feed label used for the national aggregate row."""
    return NATIONAL_LABEL
