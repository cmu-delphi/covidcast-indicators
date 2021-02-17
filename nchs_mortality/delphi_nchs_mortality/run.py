# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_nchs_mortality`.
"""
import time
from datetime import datetime, date, timedelta

import numpy as np
from delphi_utils import read_params, S3ArchiveDiffer, get_structured_logger

from .archive_diffs import arch_diffs
from .constants import (METRICS, SENSOR_NAME_MAP,
                        SENSORS, INCIDENCE_BASE, GEO_RES)
from .export import export_csv
from .pull import pull_nchs_mortality_data


def run_module():
    """Run module for processing NCHS mortality data."""
    start_time = time.time()
    params = read_params()
    logger = get_structured_logger(
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))
    export_start_date = params["indicator"]["export_start_date"]
    if export_start_date == "latest": # Find the previous Saturday
        export_start_date = date.today() - timedelta(
                days=date.today().weekday() + 2)
        export_start_date = export_start_date.strftime('%Y-%m-%d')
    daily_export_dir = params["common"]["daily_export_dir"]
    daily_cache_dir = params["indicator"]["daily_cache_dir"]
    token = params["indicator"]["token"]
    test_mode = params["indicator"]["mode"]

    if params["archive"]:
        daily_arch_diff = S3ArchiveDiffer(
            daily_cache_dir, daily_export_dir,
            params["archive"]["bucket_name"], "nchs_mortality",
            params["archive"]["aws_credentials"])
        daily_arch_diff.update_cache()


    df_pull = pull_nchs_mortality_data(token, test_mode)
    for metric in METRICS:
        if metric == 'percent_of_expected_deaths':
            print(metric)
            df = df_pull.copy()
            df["val"] = df[metric]
            df["se"] = np.nan
            df["sample_size"] = np.nan
            df = df[~df["val"].isnull()]
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
                df = df[~df["val"].isnull()]
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
    if params["archive"]:
        arch_diffs(params, daily_arch_diff)

    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    logger.info("Completed indicator run",
        elapsed_time_in_seconds = elapsed_time_in_seconds)
