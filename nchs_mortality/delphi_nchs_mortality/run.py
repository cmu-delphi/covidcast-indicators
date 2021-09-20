# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_nchs_mortality`.
"""
import time
from datetime import datetime, date, timedelta
from typing import Dict, Any
from itertools import product

import numpy as np
from delphi_utils import S3ArchiveDiffer, get_structured_logger, GeoMapper

from .archive_diffs import arch_diffs
from .constants import (METRICS, SENSOR_NAME_MAP,
                        SENSORS, INCIDENCE_BASE, GEO_RES,
                        PERCENT_EXPECTED)
from .export import export_csv
from .pull import pull_nchs_mortality_data


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
    if export_start_date == "latest":  # Find the previous Saturday
        export_start_date = date.today() - timedelta(
            days=date.today().weekday() + 2)
        export_start_date = export_start_date.strftime('%Y-%m-%d')
    daily_export_dir = params["common"]["daily_export_dir"]
    token = params["indicator"]["token"]
    test_file = params["indicator"].get("test_file", None)
    gmpr = GeoMapper()

    if "archive" in params:
        daily_arch_diff = S3ArchiveDiffer(
            params["archive"]["daily_cache_dir"], daily_export_dir,
            params["archive"]["bucket_name"], "nchs_mortality",
            params["archive"]["aws_credentials"])
        daily_arch_diff.update_cache()

    stats = []
    df_pull = pull_nchs_mortality_data(token, test_file)
    for metric, geo, sensor, in product(METRICS, GEO_RES, SENSORS):
        is_percent = metric == PERCENT_EXPECTED
        if is_percent and sensor == 'prop':
            continue

        sensor_name = [SENSOR_NAME_MAP[metric]]
        if not is_percent:
            sensor_name.append(sensor)
        print(sensor_name)
        sensor_name = "_".join(sensor_name)

        df = _safe_copy_df(df_pull, metric)

        if geo in ["hhs", "nation"]:
            df = _map_from_state(df, geo, gmpr, weighted=is_percent)

        if sensor == "prop":
            # never encountered when is_percent
            df["val"] = df["val"] / df["population"] * INCIDENCE_BASE

        dates = export_csv(
            df,
            geo_name=geo,
            export_dir=daily_export_dir,
            start_date=datetime.strptime(export_start_date, "%Y-%m-%d"),
            sensor=sensor_name,
        )
        if len(dates) > 0:
            stats.append((max(dates), len(dates)))

    # Weekly run of archive utility on Monday
    # - Does not upload to S3, that is handled by daily run of archive utility
    # - Exports issues into receiving for the API
    # Daily run of archiving utility
    # - Uploads changed files to S3
    # - Does not export any issues into receiving
    if "archive" in params:
        arch_diffs(params, daily_arch_diff)

    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    min_max_date = stats and min(s[0] for s in stats)
    csv_export_count = sum(s[-1] for s in stats)
    max_lag_in_days = min_max_date and (datetime.now() - min_max_date).days
    formatted_min_max_date = min_max_date and min_max_date.strftime("%Y-%m-%d")
    logger.info("Completed indicator run",
                elapsed_time_in_seconds=elapsed_time_in_seconds,
                csv_export_count=csv_export_count,
                max_lag_in_days=max_lag_in_days,
                oldest_final_export_date=formatted_min_max_date)


def _safe_copy_df(df, metric_col_name):
    """Create a copy of the given df, and drop rows where the metric is nan."""
    df_copy = df.copy()
    df_copy["se"] = np.nan
    df_copy["sample_size"] = np.nan
    df_copy["val"] = df_copy[metric_col_name]
    return df_copy[~df_copy["val"].isnull()]


def _map_from_state(df, geo, gmpr, weighted=False):
    """Map from state_id to another given geocode.

    The weighted flag is used when aggregating metrics which come as percentages
    rather than raw counts, and therefore need to be weighted by population when
    combining.
    """
    # TODO - this first mapping from state_id to state_code is necessary because
    #  the GeoMapper does not currently support going directly from state_id to hhs or
    #  nation. See issue #1255
    df = gmpr.replace_geocode(
        df, "state_id", "state_code", from_col="geo_id", date_col="timestamp")
    if weighted:
        df["weight"] = df["population"]
    df = gmpr.replace_geocode(
        df, "state_code", geo, data_cols=["val"], date_col="timestamp").rename(
        columns={geo: "geo_id"})
    if weighted:
        df["val"] = df["val"] / df["population"]

    return df
