# -*- coding: utf-8 -*-
"""Function to export the dataset in the format expected of the API.
"""
from datetime import datetime

import numpy as np
import pandas as pd

from .smooth import smoothed_values_by_geo_id

RESCALE_VAL = 4000 / 100


def export_csv(
    df: pd.DataFrame, geo_name: str, sensor: str, smooth: bool,
    start_date: str, receiving_dir: str
) -> None:
    """Export data set in format expected for injestion by the API

    Note that the output values will be multiplied by the value RESCALE_VAL
    defined in this file.

    Parameters
    ----------
    df: pd.DataFrame
        data frame with columns "geo_id", "timestamp", and "val"
    geo_name: str
        name of the geographic region, such as "state" or "hrr"
    sensor: str
        name of the sensor; only used for naming the output file
    smooth: bool
        should the signal in "val" be smoothed?
    start_date: str
        Output start date as a string formated as "YYYY-MM-DD"
    receiving_dir: str
        path to location where the output CSV files to be uploaded should be stored
    """

    df = df.copy()

    if smooth:
        df["val"] = smoothed_values_by_geo_id(df)

    df["val"] /= RESCALE_VAL
    df["se"] = np.nan
    df["sample_size"] = np.nan

    start_date = datetime.strptime(start_date, "%Y-%m-%d")

    for date in df["timestamp"].unique():
        if datetime.strptime(date, "%Y-%m-%d") >= start_date:
            date_short = date.replace("-", "")
            export_fn = f"{date_short}_{geo_name}_{sensor}.csv"
            df[df["timestamp"] == date][["geo_id", "val", "se", "sample_size"]].to_csv(
                f"{receiving_dir}/{export_fn}",
                index=False,
                na_rep="NA",
                float_format="%.8f",
            )
