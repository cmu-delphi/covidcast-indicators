"""Export data in the format expected by the Delphi API."""
# -*- coding: utf-8 -*-
from datetime import datetime
from os.path import join
from typing import Optional

import numpy as np
import pandas as pd

def create_export_csv(
    df: pd.DataFrame,
    export_dir: str,
    geo_res: str,
    sensor: str,
    metric: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
):
    """Export data in the format expected by the Delphi API.

    Parameters
    ----------
    df: pd.DataFrame
        Columns: geo_id, timestamp, val, se, sample_size
    export_dir: str
        Export directory
    geo_res: str
        Geographic resolution to which the data has been aggregated
    sensor: str
        Sensor that has been calculated (cumulative_counts vs new_counts)
    metric: Optional[str]
        Metric we are considering, if any.
    start_date: Optional[datetime]
        Earliest date to export or None if no minimum date restrictions should be applied.
    end_date: Optional[datetime]
        Latest date to export or None if no maximum date restrictions should be applied.
    """
    df = df.copy()

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    if start_date is None:
        start_date = min(df["timestamp"])
    if end_date is None:
        end_date = max(df["timestamp"])

    dates = pd.Series(
        df[np.logical_and(df["timestamp"] >= start_date,
                          df["timestamp"] <= end_date)]["timestamp"].unique()
    ).sort_values()

    for date in dates:
        if metric is None:
            export_filename = f"{date.strftime('%Y%m%d')}_{geo_res}_{sensor}.csv"
        else:
            export_filename = f"{date.strftime('%Y%m%d')}_{geo_res}_{metric}_{sensor}.csv"
        export_file = join(export_dir, export_filename)
        df[df["timestamp"] == date][["geo_id", "val", "se", "sample_size",]].to_csv(
            export_file, index=False, na_rep="NA"
        )
