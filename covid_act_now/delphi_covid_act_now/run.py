# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_covid_act_now`.
"""
import time

import numpy as np

from delphi_utils import (
    create_export_csv,
    S3ArchiveDiffer,
    get_structured_logger
)

from .constants import GEO_RESOLUTIONS, SIGNALS
from .geo import geo_map
from .pull import load_data, extract_testing_metrics

def run_module(params):
    """
    Run the CAN testing metrics indicator.

    Parameters
    ----------
    params
        Dictionary containing indicator configuration. Expected to have the following structure:
        - "common":
            - "export_dir": str, directory to write output
        - "indicator":
            - "parquet_url": str, URL of source file in parquet format
        - "archive" (optional): if provided, output will be archived with S3
            - "cache_dir": str, directory of locally cached data
            - "bucket_name: str, name of S3 bucket to read/write
            - "aws_credentials": Dict[str, str], AWS login credentials (see S3 documentation)
    """
    start_time = time.time()
    logger = get_structured_logger(
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))

    # Configuration
    export_dir = params["common"]["export_dir"]
    parquet_url = params["indicator"]["parquet_url"]

    # Archive Differ configuration
    if "archive" in params:
        cache_dir = params["archive"]["cache_dir"]
        arch_diff = S3ArchiveDiffer(
            cache_dir, export_dir,
            params["archive"]["bucket_name"], "CAN",
            params["archive"]["aws_credentials"])
        arch_diff.update_cache()
    else:
        arch_diff = None

    # Load CAN county-level testing data
    print("Pulling CAN data")
    df_pq = load_data(parquet_url)
    df_county_testing = extract_testing_metrics(df_pq)

    # Perform geo aggregations and export to receiving
    for geo_res in GEO_RESOLUTIONS:
        print(f"Processing {geo_res}")
        df = geo_map(df_county_testing, geo_res)

        # Export 'pcr_specimen_positivity_rate'
        exported_csv_dates = create_export_csv(
            df,
            export_dir=export_dir,
            geo_res=geo_res,
            sensor=SIGNALS[0])

        # Export 'pcr_specimen_total_tests'
        df["val"] = df["sample_size"]
        df["sample_size"] = np.nan
        df["se"] = np.nan
        exported_csv_dates = create_export_csv(
            df,
            export_dir=export_dir,
            geo_res=geo_res,
            sensor=SIGNALS[1])

        earliest, latest = min(exported_csv_dates), max(exported_csv_dates)
        files_exported = df.size * 2  # multiply by two to count both positivity and tests signals
        print(f"Exported dates: {earliest} to {latest}")

    # Perform archive differencing
    if not arch_diff is None:
        # Diff exports, and make incremental versions
        _, common_diffs, new_files = arch_diff.diff_exports()

        # Archive changed and new files only
        to_archive = [f for f, diff in common_diffs.items() if diff is not None]
        to_archive += new_files
        _, fails = arch_diff.archive_exports(to_archive)

        # Filter existing exports to exclude those that failed to archive
        succ_common_diffs = {
            f: diff for f, diff in common_diffs.items() if f not in fails
        }
        arch_diff.filter_exports(succ_common_diffs)

        # Report failures: someone should probably look at them
        for exported_file in fails:
            print(f"Failed to archive '{exported_file}'")

    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    logger.info("Completed indicator run",
                elapsed_time_in_seconds=elapsed_time_in_seconds,
                csv_export_count=files_exported,
                earliest_export_date=earliest.strftime("%Y-%m-%d"),
                latest_export_date=latest.strftime("%Y-%m-%d"))
