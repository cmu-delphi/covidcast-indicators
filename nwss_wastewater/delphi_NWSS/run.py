# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.  `run_module`'s lone argument should be a
nested dictionary of parameters loaded from the params.json file.  We expect the `params` to have
the following structure:
    - "common":
        - "daily_export_dir": str, directory to write daily output
        - "weekly_export_dir": str, directory to write weekly output
        - "log_filename": (optional) str, path to log file
        - "log_exceptions" (optional): bool, whether to log exceptions to file
    - "indicator": (optional)
        - "wip_signal": (optional) Any[str, bool], list of signals that are
            works in progress, or True if all signals in the registry are works
            in progress, or False if only unpublished signals are.  See
            `delphi_utils.add_prefix()`
        - "test_file" (optional): str, name of file from which to read test data
        - "token": str, authentication for upstream data pull
    - "archive" (optional): if provided, output will be archived with S3
        - "aws_credentials": Dict[str, str], AWS login credentials (see S3 documentation)
        - "bucket_name: str, name of S3 bucket to read/write
        - "daily_cache_dir": str, directory of locally cached daily data
        - "weekly_cache_dir": str, directory of locally cached weekly data
"""
import time
import os
from datetime import timedelta, datetime
from itertools import product

import numpy as np
import pandas as pd
from delphi_utils import S3ArchiveDiffer, get_structured_logger, create_export_csv, Nans
from delphi_utils.geomap import GeoMapper

from .constants import GEOS, SIGNALS, SMOOTHERS
from .pull import pull_nwss_data


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


def generate_weights(df, column_aggregating="pcr_conc_smoothed"):
    # set the weight of places with na's to zero
    df[f"relevant_pop_{column_aggregating}"] = (
        df["population_served"] * df[column_aggregating].notna()
    )
    # generate the weighted version
    df[f"weighted_{column_aggregating}"] = (
        df[column_aggregating] * df[f"relevant_pop_{column_aggregating}"]
    )
    return df


def add_needed_columns(df, col_names=["se", "sample_size"]):
    for col_name in col_names:
        df[col_name] = np.nan
    df = add_nancodes(df)
    return df


def run_module(params: dict[str, any]):
    """
    Runs the indicator

    Arguments
    --------
    params:  Dict[str, Any]
        Nested dictionary of parameters.
    """
    start_time = time.time()
    logger = get_structured_logger(
        __name__,
        filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True),
    )
    export_start_date = params["indicator"]["export_start_date"]
    daily_export_dir = params["common"]["daily_export_dir"]
    token = params["indicator"]["token"]
    test_file = params["indicator"].get("test_file", None)
    if "archive" in params:
        daily_arch_diff = S3ArchiveDiffer(
            params["archive"]["daily_cache_dir"],
            daily_export_dir,
            params["archive"]["bucket_name"],
            "nchs_mortality",
            params["archive"]["aws_credentials"],
        )
        daily_arch_diff.update_cache()

    run_stats = []
    ## build the base version of the signal at the most detailed geo level you can get.
    ## compute stuff here or farm out to another function or file
    df_pull = pull_nwss_data(token)
    start_date = min(df_pull["timestamp"])
    ## aggregate
    for sensor in SIGNALS:
        df = df_pull.copy()
        # add weighed column
        df = generate_weights(df, sensor)

        for geo in GEOS:
            logger.info("Generating signal and exporting to CSV", metric=sensor)
            if geo == "nation":
                agg_df = df.groupby("timestamp").agg(
                    {"population_served": "sum", f"weighted_{sensor}": "sum"}
                )
                agg_df["val"] = agg_df[f"weighted_{sensor}"] / agg_df.population_served
                agg_df = agg_df.reset_index()
                agg_df["geo_id"] = "us"
            else:
                agg_df = df.groupby(["timestamp", geo]).agg(
                    {"population_served": "sum", f"weighted_{sensor}": "sum"}
                )
                agg_df["val"] = agg_df[f"weighted_{sensor}"] / agg_df.population_served
                agg_df = agg_df.reset_index()
                agg_df = agg_df.rename(columns={"state": "geo_id"})
            # add se, sample_size, and na codes
            agg_df = add_needed_columns(agg_df)
            print(agg_df)
            # actual export
            dates = create_export_csv(
                agg_df, geo_res=geo, export_dir=daily_export_dir, sensor=sensor
            )
            if len(dates) > 0:
                run_stats.append((max(dates), len(dates)))
    ## log this indicator run
    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    min_max_date = run_stats and min(s[0] for s in run_stats)
    csv_export_count = sum(s[-1] for s in run_stats)
    max_lag_in_days = min_max_date and (datetime.now() - min_max_date).days
    formatted_min_max_date = min_max_date and min_max_date.strftime("%Y-%m-%d")
    logger.info(
        "Completed indicator run",
        elapsed_time_in_seconds=elapsed_time_in_seconds,
        csv_export_count=csv_export_count,
        max_lag_in_days=max_lag_in_days,
        oldest_final_export_date=formatted_min_max_date,
    )
