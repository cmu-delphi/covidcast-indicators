# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_nchs_mortality`.
"""
import time
from datetime import datetime, date, timedelta
from typing import Dict, Any

import numpy as np
from delphi_utils import S3ArchiveDiffer, get_structured_logger, Nans

from .archive_diffs import arch_diffs
from .constants import (METRICS, SENSOR_NAME_MAP,
                        SENSORS, INCIDENCE_BASE, GEO_RES)
from .export import export_csv
from .pull import pull_nchs_mortality_data


def add_nancodes(df):
    """Add nancodes to the dataframe."""
    # Default missingness codes
    df["missing_val"] = Nans.NOT_MISSING
    df["missing_se"] = Nans.NOT_APPLICABLE
    df["missing_sample_size"] = Nans.NOT_APPLICABLE

    # Mark any remaining nans with unknown
    remaining_nans_mask = df["val"].isnull()
    df.loc[remaining_nans_mask, "missing_val"] = Nans.UNKNOWN
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
        - "token": str, authentication for upstream data pull
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
    token = params["indicator"]["token"]
    test_file = params["indicator"].get("test_file", None)

    if "archive" in params:
        daily_arch_diff = S3ArchiveDiffer(
            params["archive"]["daily_cache_dir"], daily_export_dir,
            params["archive"]["bucket_name"], "nchs_mortality",
            params["archive"]["aws_credentials"])
        daily_arch_diff.update_cache()


    df_pull = pull_nchs_mortality_data(token, test_file)
    for metric in METRICS:
        if metric == 'percent_of_expected_deaths':
            print(metric)
            df = df_pull.copy()
            df["val"] = df[metric]
            df["se"] = np.nan
            df["sample_size"] = np.nan
            df = add_nancodes(df)
            # df = df[~df["val"].isnull()]
            sensor_name = "_".join([SENSOR_NAME_MAP[metric]])
            export_csv(
                df,
                geo_name=GEO_RES,
                export_dir=daily_export_dir,
                start_date=datetime.strptime(export_start_date, "%Y-%m-%d"),
                sensor=sensor_name,
            )
        else:
            for sensor in SENSORS:
                print(metric, sensor)
                df = df_pull.copy()
                if sensor == "num":
                    df["val"] = df[metric]
                else:
                    df["val"] = df[metric] / df["population"] * INCIDENCE_BASE
                df["se"] = np.nan
                df["sample_size"] = np.nan
                df = add_nancodes(df)
                # df = df[~df["val"].isnull()]
                sensor_name = "_".join([SENSOR_NAME_MAP[metric], sensor])
                export_csv(
                    df,
                    geo_name=GEO_RES,
                    export_dir=daily_export_dir,
                    start_date=datetime.strptime(export_start_date, "%Y-%m-%d"),
                    sensor=sensor_name,
                )

#     Weekly run of archive utility on Monday
#     - Does not upload to S3, that is handled by daily run of archive utility
#     - Exports issues into receiving for the API
#     Daily run of archiving utility
#     - Uploads changed files to S3
#     - Does not export any issues into receiving
    if "archive" in params:
        arch_diffs(params, daily_arch_diff)

    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    logger.info("Completed indicator run",
        elapsed_time_in_seconds = elapsed_time_in_seconds)
