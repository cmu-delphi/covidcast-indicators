# -*- coding: utf-8 -*-
from datetime import datetime
from os.path import join

import pandas as pd

def create_export_csv(
    df: pd.DataFrame,
    start_date: datetime,
    export_dir: str,
    metric: str,
    geo_res: str,
    sensor: str,
):
    """Export data in the format expected by the Delphi API.

    Parameters
    ----------
    df: pd.DataFrame
        Columns: geo_id, timestamp, val, se, sample_size
    export_dir: str
        Export directory
    metric: str
        Metric we are considering
    geo_res: str
        Geographic resolution to which the data has been aggregated
    sensor: str
        Sensor that has been calculated (cumulative_counts vs new_counts)
    """
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    dates = pd.Series(
        df[df["timestamp"] >= start_date]["timestamp"].unique()
    ).sort_values()
    for date in dates:
        export_fn = f'{date.strftime("%Y%m%d")}_{geo_res}_' f"{metric}_{sensor}.csv"
        export_file = join(export_dir, export_fn)
        df[df["timestamp"] == date][["geo_id", "val", "se", "sample_size",]].to_csv(
            export_file, index=False, na_rep="NA"
        )
