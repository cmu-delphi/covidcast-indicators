"""Functions for generating signals."""

from typing import Callable

import pandas as pd
import numpy as np


def generate_signal(df: pd.DataFrame,
                    input_cols: list,
                    signal_func: Callable,
                    date_offset: int) -> pd.DataFrame:
    """
    Generate a signal DataFrame derived from an input DataFrame.

    Applies the provided function on the columns specified, and then aggregates by geo and time.

    Parameters
    ----------
    df: pd.DataFrame
        Input DataFrame containing columns specified in `input_cols`.
    input_cols: list of strings
        List of column names to pass to `signal_func`.
    signal_func: function
        Function which takes in a list of Series and produces a signal Series.
    date_offset: integer
        Number of days to add to the timestamp. This is used because some of the columns are
        "previous_day_<metric>" and require us adding -1 days to represent the right timespan.

    Returns
    -------
    Signal DataFrame that is ready for `create_export_csv`.
    """
    df_cols = [df[i] for i in input_cols]
    df["val"] = signal_func(df_cols)
    df["timestamp"] = df["timestamp"] + pd.Timedelta(days=date_offset)
    df.dropna(subset=["val"], inplace=True)
    df = df.groupby(["timestamp", "geo_id"], as_index=False).sum()
    df["se"] = df["sample_size"] = np.nan
    return df[["timestamp", "geo_id", "val", "se", "sample_size"]]


def sum_cols(cols: list) -> pd.Series:
    """
    Sum a list of Series, requiring 1 non-nan value per row sum.

    Parameters
    ----------
    cols: list of Series
        List of Series to sum.

    Returns
    -------
    Series of summed inputs.
    """
    return pd.concat(cols, axis=1).sum(axis=1, min_count=1)
