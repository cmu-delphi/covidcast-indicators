# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_nchs_mortality`.
"""
from datetime import datetime, date, timedelta
from os.path import join

import numpy as np
import pandas as pd
from delphi_utils import read_params, S3ArchiveDiffer

from .pull import pull_nchs_mortality_data
from .export import export_csv
from .archive_diffs import arch_diffs
from .constants import (METRICS, SENSOR_NAME_MAP,
                        SENSORS, INCIDENCE_BASE, GEO_RES)

def run_module():
    """Run module for processing NCHS mortality data."""
    params = read_params()
    export_start_date = params["export_start_date"]
    if export_start_date == "latest": # Find the previous Saturday
        export_start_date = date.today() - timedelta(
                days=date.today().weekday() + 2)
        export_start_date = export_start_date.strftime('%Y-%m-%d')
    daily_export_dir = params["daily_export_dir"]
    daily_cache_dir = params["daily_cache_dir"]
    static_file_dir = params["static_file_dir"]
    token = params["token"]
    test_mode = params["mode"]

    daily_arch_diff = S3ArchiveDiffer(
        daily_cache_dir, daily_export_dir,
        params["bucket_name"], "nchs_mortality",
        params["aws_credentials"])
    daily_arch_diff.update_cache()

    map_df = pd.read_csv(
        join(static_file_dir, "state_pop.csv"), dtype={"fips": int}
    )

    df = pull_nchs_mortality_data(token, map_df, test_mode)
    for metric in METRICS:
        if metric == 'percent_of_expected_deaths':
            print(metric)
            df["val"] = df[metric]
            df["se"] = np.nan
            df["sample_size"] = np.nan
            df = df[~df["val"].isnull()]
            sensor_name = "_".join(["wip", SENSOR_NAME_MAP[metric]])
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
                if sensor == "num":
                    df["val"] = df[metric]
                else:
                    df["val"] = df[metric] / df["population"] * INCIDENCE_BASE
                df["se"] = np.nan
                df["sample_size"] = np.nan
                df = df[~df["val"].isnull()]
                sensor_name = "_".join(["wip", SENSOR_NAME_MAP[metric], sensor])
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
    arch_diffs(params, daily_arch_diff)
