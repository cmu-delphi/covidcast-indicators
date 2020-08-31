# -*- coding: utf-8 -*-
"""Function to export the dataset in the format expected of the API.
"""
import numpy as np
import pandas as pd

def export_csv(df, geo_name, sensor, receiving_dir, start_date, end_date):
    """Export data set in format expected for injestion by the API
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
    receiving_dir: str
        path to location where the output CSV files to be uploaded should be stored
    start_date: datetime.datetime
        The first date to report
    end_date: datetime.datetime
        The last date to report
    """

    df = df.copy()
    df = df[np.logical_and(df["timestamp"] >= start_date,
                           df["timestamp"] <= end_date)]

    for date in df["timestamp"].unique():
        t = pd.to_datetime(str(date))
        date_short = t.strftime('%Y%m%d')
        export_fn = f"{date_short}_{geo_name}_{sensor}.csv"
        result_df = df[df["timestamp"] == date][["geo_id", "val", "se", "sample_size"]]
        result_df = result_df[result_df["sample_size"].notnull()]
        result_df.to_csv(f"{receiving_dir}/{export_fn}",
                         index=False,
                         float_format="%.8f")
