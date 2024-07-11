"""Small utility functions"""

import time
from datetime import datetime

import numpy as np
from delphi_utils.nancodes import add_default_nancodes

from .constants import AUXILIARY_COLS


def add_needed_columns(df, col_names=None):
    """Short util to add expected columns not found in the dataset."""
    if col_names is None:
        col_names = AUXILIARY_COLS

    for col_name in col_names:
        df[col_name] = np.nan
    df = add_default_nancodes(df)
    return df


def summary_log(start_time, run_stats, logger):
    """Boilerplate for final logging statement."""
    min_max_date = run_stats and min(s[0] for s in run_stats)
    max_lag_in_days = min_max_date and (datetime.now() - min_max_date).days
    formatted_min_max_date = min_max_date and min_max_date.strftime("%Y-%m-%d")

    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    csv_export_count = sum(s[-1] for s in run_stats)

    logger.info(
        "Completed indicator run",
        elapsed_time_in_seconds=elapsed_time_in_seconds,
        csv_export_count=csv_export_count,
        max_lag_in_days=max_lag_in_days,
        oldest_final_export_date=formatted_min_max_date,
    )
