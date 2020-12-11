"""Functions for generating signals."""

from typing import Callable

import pandas as pd
import numpy as np


def generate_signal(df: pd.DataFrame,
                    input_cols: list,
                    signal_func: Callable,
                    date_offset: int) -> pd.DataFrame:
    """Run a signal generation function on a df."""
    df_cols = [df[i] for i in input_cols]
    df["val"] = signal_func(df_cols)
    df["timestamp"] = df["timestamp"] + pd.Timedelta(days=date_offset)
    df = df.groupby(["timestamp", "geo_id"], as_index=False).sum()
    df["se"] = np.nan
    df["sample_size"] = np.nan
    return df


def sum_cols(cols: list) -> pd.Series:
    """Sum a list of Series, requiring 1 non-nan value per row sum."""
    return pd.concat(cols, axis=1).sum(axis=1, min_count=1)
