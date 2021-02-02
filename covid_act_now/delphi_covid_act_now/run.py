# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_covid_act_now`.
"""

from delphi_utils import (
    read_params,
    create_export_csv,
    S3ArchiveDiffer,
)

from .constants import GEO_RESOLUTIONS, SIGNALS
from .geo import geo_map
from .pull import load_data, extract_testing_metrics

def run_module():
    """Run the CAN testing metrics indicator."""
    # Configuration
    params = read_params()
    export_dir = params["export_dir"]
    cache_dir = params["cache_dir"]
    parquet_url = params["parquet_url"]

    # Archive Differ configuration
    if len(params["bucket_name"]) > 0:
        arch_diff = S3ArchiveDiffer(
            cache_dir, export_dir,
            params["bucket_name"], "CAN",
            params["aws_credentials"])
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

        # Only 1 signal for now
        signal = SIGNALS[0]

        exported_csv_dates = create_export_csv(
            df,
            export_dir=export_dir,
            geo_res=geo_res,
            sensor=signal)

        earliest, latest = min(exported_csv_dates), max(exported_csv_dates)
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
