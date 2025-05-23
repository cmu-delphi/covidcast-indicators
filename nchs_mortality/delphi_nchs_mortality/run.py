# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_nchs_mortality`.
"""
import time
from datetime import datetime, date, timedelta
from typing import Dict, Any

import numpy as np
from delphi_utils import S3ArchiveDiffer, get_structured_logger, create_export_csv, Nans

from .archive_diffs import arch_diffs
from .constants import (METRICS, SENSOR_NAME_MAP,
                        SENSORS, INCIDENCE_BASE)
from .pull import pull_nchs_mortality_data


def add_nancodes(df):
    """Add nancodes to the dataframe."""
    # Default missingness codes
    df["missing_val"] = Nans.NOT_MISSING
    df["missing_se"] = Nans.NOT_APPLICABLE
    df["missing_sample_size"] = Nans.NOT_APPLICABLE

    # Mark any remaining nans with unknown
    remaining_nans_mask = df["val"].isnull()
    df.loc[remaining_nans_mask, "missing_val"] = Nans.OTHER
    return df

def run_module(params: Dict[str, Any]):
    """Run module for processing NCHS mortality data.

    The `params` argument is expected to have the following structure:
    - "common":
        - "daily_export_dir": str, directory to write daily output
        - "weekly_export_dir": str, directory to write weekly output
        - "log_exceptions" (optional): bool, whether to log exceptions to file
        - "log_filename" (optional): str, name of file to write logs
    - "indicator":
        - "export_start_date": str, date from which to export data in YYYY-MM-DD format
        - "static_file_dir": str, directory containing population csv files
        - "test_file" (optional): str, name of file from which to read test data
        - "socrata_token": str, authentication for upstream data pull
    - "archive" (optional): if provided, output will be archived with S3
        - "aws_credentials": Dict[str, str], AWS login credentials (see S3 documentation)
        - "bucket_name: str, name of S3 bucket to read/write
        - "daily_cache_dir": str, directory of locally cached daily data
        - "weekly_cache_dir": str, directory of locally cached weekly data
    """
    start_time = time.time()
    logger = get_structured_logger(
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))
    export_start_date = params["indicator"]["export_start_date"]
    if export_start_date == "latest": # Find the previous Saturday
        export_start_date = date.today() - timedelta(
                days=date.today().weekday() + 2)
        export_start_date = export_start_date.strftime('%Y-%m-%d')
    daily_export_dir = params["common"]["daily_export_dir"]
    backup_dir = params["common"]["backup_dir"]
    custom_run = params["common"].get("custom_run", False)
    socrata_token = params["indicator"]["socrata_token"]
    test_file = params["indicator"].get("test_file", None)

    if "archive" in params:
        daily_arch_diff = S3ArchiveDiffer(
            params["archive"]["daily_cache_dir"], daily_export_dir,
            params["archive"]["bucket_name"], "nchs_mortality",
            params["archive"]["aws_credentials"])
        daily_arch_diff.update_cache()

    stats = []
    df_pull = pull_nchs_mortality_data(
        socrata_token, backup_dir, custom_run=custom_run, test_file=test_file, logger=logger
    )
    for metric in METRICS:
        for geo in ["state", "nation"]:
            if metric == 'percent_of_expected_deaths':
                logger.info("Generating signal and exporting to CSV",
                            metric=metric, geo_level=geo)
                df = df_pull.copy()
                if geo == "nation":
                    df = df[df["geo_id"] == "us"]
                else:
                    df = df[df["geo_id"] != "us"]
                df["val"] = df[metric]
                df["se"] = np.nan
                df["sample_size"] = np.nan
                df = add_nancodes(df)
                dates = create_export_csv(
                    df,
                    geo_res=geo,
                    export_dir=daily_export_dir,
                    start_date=datetime.strptime(export_start_date, "%Y-%m-%d"),
                    sensor=SENSOR_NAME_MAP[metric],
                    weekly_dates=True
                )
            else:
                for sensor in SENSORS:
                    logger.info("Generating signal and exporting to CSV",
                                metric=metric, sensor=sensor, geo_level=geo)
                    df = df_pull.copy()
                    if geo == "nation":
                        df = df[df["geo_id"] == "us"]
                    else:
                        df = df[df["geo_id"] != "us"]
                    if sensor == "num":
                        df["val"] = df[metric]
                    else:
                        df["val"] = df[metric] / df["population"] * INCIDENCE_BASE
                    df["se"] = np.nan
                    df["sample_size"] = np.nan
                    df = add_nancodes(df)
                    sensor_name = "_".join([SENSOR_NAME_MAP[metric], sensor])
                    dates = create_export_csv(
                        df,
                        geo_res=geo,
                        export_dir=daily_export_dir,
                        start_date=datetime.strptime(export_start_date, "%Y-%m-%d"),
                        sensor=sensor_name,
                        weekly_dates=True
                    )
            if len(dates) > 0:
                stats.append((max(dates), len(dates)))

#     Weekly run of archive utility on Monday
#     - Does not upload to S3, that is handled by daily run of archive utility
#     - Exports issues into receiving for the API
#     Daily run of archiving utility
#     - Uploads changed files to S3
#     - Does not export any issues into receiving
    if "archive" in params:
        arch_diffs(params, daily_arch_diff, logger)

    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    min_max_date = stats and min(s[0] for s in stats)
    csv_export_count = sum(s[-1] for s in stats)
    max_lag_in_days = min_max_date and (datetime.now() - min_max_date).days
    formatted_min_max_date = min_max_date and min_max_date.strftime("%Y-%m-%d")
    logger.info("Completed indicator run",
                elapsed_time_in_seconds = elapsed_time_in_seconds,
                csv_export_count = csv_export_count,
                max_lag_in_days = max_lag_in_days,
                oldest_final_export_date = formatted_min_max_date)
