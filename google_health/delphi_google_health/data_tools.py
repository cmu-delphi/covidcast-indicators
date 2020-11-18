# -*- coding: utf-8 -*-
"""Functions to reformat the data."""

import numpy as np
import pandas as pd

from .smooth import smoothed_values_by_geo_id

RESCALE_VAL = 4000 / 100

def format_for_export(df: pd.DataFrame, smooth: bool):
    """Transform data columns of df to match those expected by `delphi_utils.create_export_csv()`.
    Parameters
    ----------
    df: pd.DataFrame
        data frame with columns "geo_id", "timestamp", and "val"
    smooth: bool
        should the signal in "val" be smoothed?

    Returns
    -------
    pd.DataFrame
        A data frame with columns "val", "se", and "sample_size".
    """
    df = df.copy()
    if smooth:
        df["val"] = smoothed_values_by_geo_id(df)

    df["val"] /= RESCALE_VAL
    df["se"] = np.nan
    df["sample_size"] = np.nan
    return df
