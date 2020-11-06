# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.
"""
from datetime import datetime, date, timedelta
from os.path import join
from os import remove, listdir
from shutil import copy

import numpy as np
import pandas as pd
from delphi_utils import read_params, S3ArchiveDiffer

from .pull import pull_nchs_mortality_data
from .export import export_csv

# global constants
METRICS = [
        'covid_deaths', 'total_deaths', 'percent_of_expected_deaths',
        'pneumonia_deaths', 'pneumonia_and_covid_deaths', 'influenza_deaths',
        'pneumonia_influenza_or_covid_19_deaths'
]
SENSORS = [
        "num",
        "prop"
]
INCIDENCE_BASE = 100000
GEO_RES = "state"

def run_module():  # pylint: disable=too-many-branches,too-many-statements
    """Run module for processing NCHS mortality data."""
    params = read_params()
    export_start_date = params["export_start_date"]
    if export_start_date == "latest": # Find the previous Saturday
        export_start_date = date.today() - timedelta(
                days=date.today().weekday() + 2)
        export_start_date = export_start_date.strftime('%Y-%m-%d')
    export_dir = params["export_dir"]
    daily_export_dir = params["daily_export_dir"]
    cache_dir = params["cache_dir"]
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
            sensor_name = "_".join(["wip", metric])
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
                sensor_name = "_".join(["wip", metric, sensor])
                export_csv(
                    df,
                    geo_name=GEO_RES,
                    export_dir=daily_export_dir,
                    start_date=datetime.strptime(export_start_date, "%Y-%m-%d"),
                    sensor=sensor_name,
                )

    # Weekly run of archive utility on Monday
    # - Does not upload to S3, that is handled by daily run of archive utility
    # - Exports issues into receiving for the API
    if datetime.today().weekday() == 0:
        # Copy todays raw output to receiving
        for output_file in listdir(daily_export_dir):
            copy(
                join(daily_export_dir, output_file),
                join(export_dir, output_file))

        weekly_arch_diff = S3ArchiveDiffer(
            cache_dir, export_dir,
            params["bucket_name"], "nchs_mortality",
            params["aws_credentials"])

        # Dont update cache from S3 (has daily files), only simulate a update_cache() call
        weekly_arch_diff._cache_updated = True  # pylint: disable=protected-access

        # Diff exports, and make incremental versions
        _, common_diffs, new_files = weekly_arch_diff.diff_exports()

        # Archive changed and new files only
        to_archive = [f for f, diff in common_diffs.items() if diff is not None]
        to_archive += new_files
        _, fails = weekly_arch_diff.archive_exports(to_archive, update_s3=False)

        # Filter existing exports to exclude those that failed to archive
        succ_common_diffs = {f: diff for f, diff in common_diffs.items() if f not in fails}
        weekly_arch_diff.filter_exports(succ_common_diffs)

        # Report failures: someone should probably look at them
        for exported_file in fails:
            print(f"Failed to archive (weekly) '{exported_file}'")

    # Daily run of archiving utility
    # - Uploads changed files to S3
    # - Does not export any issues into receiving

    # Diff exports, and make incremental versions
    _, common_diffs, new_files = daily_arch_diff.diff_exports()

    # Archive changed and new files only
    to_archive = [f for f, diff in common_diffs.items() if diff is not None]
    to_archive += new_files
    _, fails = daily_arch_diff.archive_exports(to_archive)

    # Daily output not needed anymore, remove them
    for exported_file in new_files:
        remove(exported_file)
    for exported_file, diff_file in common_diffs.items():
        remove(exported_file)
        remove(diff_file)

    # Report failures: someone should probably look at them
    for exported_file in fails:
        print(f"Failed to archive (daily) '{exported_file}'")
