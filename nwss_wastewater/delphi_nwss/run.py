# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.  `run_module`'s lone argument should be a
nested dictionary of parameters loaded from the params.json file.  We expect the `params` to have
the following structure:
    - "common":
        - "export_dir": str, directory to write daily output
        - "log_filename": (optional) str, path to log file
        - "log_exceptions" (optional): bool, whether to log exceptions to file
    - "indicator": (optional)
        - "wip_signal": (optional) Any[str, bool], list of signals that are
            works in progress, or True if all signals in the registry are works
            in progress, or False if only unpublished signals are.  See
            `delphi_utils.add_prefix()`
        - "test_file" (optional): str, name of file from which to read test data
        - "socrata_token": str, authentication for upstream data pull
    - "archive" (optional): if provided, output will be archived with S3
        - "aws_credentials": Dict[str, str], AWS login credentials (see S3 documentation)
        - "bucket_name: str, name of S3 bucket to read/write
        - "cache_dir": str, directory of locally cached data
"""
import time
from datetime import datetime

import numpy as np
import pandas as pd
from delphi_utils import S3ArchiveDiffer, get_structured_logger, create_export_csv
from delphi_utils.nancodes import add_default_nancodes

from .constants import GEOS, SIGNALS
from .pull import pull_nwss_data


def sum_all_nan(x):
    """Return a normal sum unless everything is NaN, then return that."""
    all_nan = np.isnan(x).all()
    if all_nan:
        return np.nan
    return np.nansum(x)


def generate_weights(df, column_aggregating="pcr_conc_smoothed"):
    """
    Weigh column_aggregating by population.

    generate the relevant population amounts, and create a weighted but
    unnormalized column, derived from `column_aggregating`
    """
    # set the weight of places with na's to zero
    df[f"relevant_pop_{column_aggregating}"] = (
        df["population_served"] * df[column_aggregating].notna()
    )
    # generate the weighted version
    df[f"weighted_{column_aggregating}"] = (
        df[column_aggregating] * df[f"relevant_pop_{column_aggregating}"]
    )
    return df


def weighted_state_sum(df: pd.DataFrame, geo: str, sensor: str):
    """Sum sensor, weighted by population for non NA's, grouped by state."""
    agg_df = df.groupby(["timestamp", geo]).agg(
        {f"relevant_pop_{sensor}": "sum", f"weighted_{sensor}": sum_all_nan}
    )
    agg_df["val"] = agg_df[f"weighted_{sensor}"] / agg_df[f"relevant_pop_{sensor}"]
    agg_df = agg_df.reset_index()
    agg_df = agg_df.rename(columns={"state": "geo_id"})
    return agg_df


def weighted_nation_sum(df: pd.DataFrame, sensor: str):
    """Sum sensor, weighted by population for non NA's."""
    agg_df = df.groupby("timestamp").agg(
        {f"relevant_pop_{sensor}": "sum", f"weighted_{sensor}": sum_all_nan}
    )
    agg_df["val"] = agg_df[f"weighted_{sensor}"] / agg_df[f"relevant_pop_{sensor}"]
    agg_df = agg_df.reset_index()
    agg_df["geo_id"] = "us"
    return agg_df


def add_needed_columns(df, col_names=None):
    """Short util to add expected columns not found in the dataset."""
    if col_names is None:
        col_names = ["se", "sample_size"]

    for col_name in col_names:
        df[col_name] = np.nan
    df = add_default_nancodes(df)
    return df


def logging(start_time, run_stats, logger):
    """Boilerplate making logs."""
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


def run_module(params):
    """
    Run the indicator.

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
    export_dir = params["common"]["export_dir"]
    socrata_token = params["indicator"]["socrata_token"]
    if "archive" in params:
        daily_arch_diff = S3ArchiveDiffer(
            params["archive"]["cache_dir"],
            export_dir,
            params["archive"]["bucket_name"],
            "nchs_mortality",
            params["archive"]["aws_credentials"],
        )
        daily_arch_diff.update_cache()

    run_stats = []
    ## build the base version of the signal at the most detailed geo level you can get.
    ## compute stuff here or farm out to another function or file
    df_pull = pull_nwss_data(socrata_token)
    ## aggregate
    for sensor in SIGNALS:
        df = df_pull.copy()
        # add weighed column
        df = generate_weights(df, sensor)

        for geo in GEOS:
            logger.info("Generating signal and exporting to CSV", geo=geo, signal=sensor)
            if geo == "nation":
                agg_df = weighted_nation_sum(df, sensor)
            else:
                agg_df = weighted_state_sum(df, geo, sensor)
            # add se, sample_size, and na codes
            agg_df = add_needed_columns(agg_df)
            # actual export
            dates = create_export_csv(
                agg_df, geo_res=geo, export_dir=export_dir, sensor=sensor
            )
            if len(dates) > 0:
                run_stats.append((max(dates), len(dates)))
    ## log this indicator run
    logging(start_time, run_stats, logger)
