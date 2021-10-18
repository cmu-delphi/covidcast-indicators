"""Export data in the format expected by the Delphi API."""
# -*- coding: utf-8 -*-
from datetime import datetime
from os.path import join
from typing import Optional
import logging

from epiweeks import Week
import numpy as np
import pandas as pd

from .nancodes import Nans

def filter_contradicting_missing_codes(df, sensor, metric, date, logger=None):
    """Find values with contradictory missingness codes, filter them, and log."""
    columns = ["val", "se", "sample_size"]
    # Get indicies where the XNOR is true (i.e. both are true or both are false).
    masks = [
        ~(df[column].isna() ^ df["missing_" + column].eq(Nans.NOT_MISSING))
        for column in columns
    ]
    for mask in masks:
        if not logger is None and df.loc[mask].size > 0:
            logger.info(
                "Filtering contradictory missing code in " +
                "{0}_{1}_{2}.".format(sensor, metric, date.strftime(format="%Y-%m-%d"))
            )
            df = df.loc[~mask]
        elif logger is None and df.loc[mask].size > 0:
            df = df.loc[~mask]
    return df

def create_export_csv(
    df: pd.DataFrame,
    export_dir: str,
    geo_res: str,
    sensor: str,
    metric: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    remove_null_samples: Optional[bool] = False,
    write_empty_days: Optional[bool] = False,
    logger: Optional[logging.Logger] = None,
    weekly_dates = False,
):
    """Export data in the format expected by the Delphi API.

    This function will round the signal and standard error values to 7 decimals places.

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
    remove_null_samples: Optional[bool]
        Whether to remove entries whose sample sizes are null.
    write_empty_days: Optional[bool]
        If true, every day in between start_date and end_date will have a CSV file written
        even if there is no data for the day. If false, only the days present are written.
    logger: Optional[logging.Logger]
        Pass a logger object here to log information about contradictory missing codes.
    weekly_dates: Optional[bool]
        Whether the output data are weekly or not. If True, will prefix files with
        "weekly_YYYYWW" where WW is the epiweek instead of the usual YYYYMMDD for daily files.

    Returns
    ---------
    dates: pd.Series[datetime]
        Series of dates for which CSV files were exported.
    """
    df = df.copy()

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    if start_date is None:
        start_date = min(df["timestamp"])
    if end_date is None:
        end_date = max(df["timestamp"])
    if not write_empty_days:
        dates = pd.Series(
            df[np.logical_and(df["timestamp"] >= start_date,
                          df["timestamp"] <= end_date)]["timestamp"].unique()
        ).sort_values()
    else:
        dates = pd.date_range(start_date, end_date)

    for date in dates:
        if weekly_dates:
            t = Week.fromdate(pd.to_datetime(str(date)))
            date_str = "weekly_" + str(t.year) + str(t.week).zfill(2)
        else:
            date_str = date.strftime('%Y%m%d')
        if metric is None:
            export_filename = f"{date_str}_{geo_res}_{sensor}.csv"
        else:
            export_filename = f"{date_str}_{geo_res}_{metric}_{sensor}.csv"
        export_file = join(export_dir, export_filename)
        expected_columns = [
            "geo_id",
            "val",
            "se",
            "sample_size",
            "missing_val",
            "missing_se",
            "missing_sample_size"
        ]
        export_df = df[df["timestamp"] == date].filter(items=expected_columns)
        if "missing_val" in export_df.columns:
            export_df = filter_contradicting_missing_codes(
                export_df, sensor, metric, date, logger=logger
            )
        if remove_null_samples:
            export_df = export_df[export_df["sample_size"].notnull()]
        export_df = export_df.round({"val": 7, "se": 7})
        export_df.to_csv(export_file, index=False, na_rep="NA")
    return dates
