# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.
"""
from datetime import datetime
from itertools import product
import time
from typing import Dict, Any

import numpy as np
from delphi_utils import (
    create_export_csv,
    Smoother,
    GeoMapper,
    get_structured_logger,
)

from .geo import geo_map
from .pull import pull_jhu_data

# global constants
METRICS = [
    "deaths",
    "confirmed",
]
SENSORS = [
    "new_counts",
    "cumulative_counts",
    "incidence",  # new_counts per 100k people
    "cumulative_prop",
]
SMOOTHERS = [
    "unsmoothed",
    "seven_day_average",
]
SENSOR_NAME_MAP = {
    "new_counts": ("incidence_num", False),
    "cumulative_counts": ("cumulative_num", False),
    "incidence": ("incidence_prop", False),
    "cumulative_prop": ("cumulative_prop", False),
}
# Temporarily added for wip_ signals
# WIP_SENSOR_NAME_MAP = {
#     "new_counts":           ("incid_num", False),
#     "cumulative_counts":    ("cumul_num", False),
#     "incidence":            ("incid_prop", False),
#     "cumulative_prop":      ("cumul_prop", False),
# }
SMOOTHERS_MAP = {
    "unsmoothed": (Smoother("identity").smooth, ""),
    "seven_day_average": (Smoother("moving_average", window_length=7).smooth, "7dav_"),
}
GEO_RESOLUTIONS = [
    "county",
    "state",
    "msa",
    "hrr",
    "hhs",
    "nation"
]


def run_module(params: Dict[str, Any]):
    """Run the JHU indicator module.

    The `params` argument is expected to have the following structure:
    - "common":
        - "export_dir": str, directory to write output
        - "log_exceptions" (optional): bool, whether to log exceptions to file
        - "log_filename" (optional): str, name of file to write logs
    - "indicator":
        - "base_url": str, URL from which to read upstream data
        - "export_start_date": str, date from which to export data in YYYY-MM-DD format
    - "archive" (optional): if provided, output will be archived with S3
        - "aws_credentials": Dict[str, str], AWS login credentials (see S3 documentation)
        - "bucket_name: str, name of S3 bucket to read/write
        - "cache_dir": str, directory of locally cached data
    """
    start_time = time.time()
    csv_export_count = 0
    oldest_final_export_date = None
    export_start_date = params["indicator"]["export_start_date"]
    export_dir = params["common"]["export_dir"]
    base_url = params["indicator"]["base_url"]
    logger = get_structured_logger(
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))

    gmpr = GeoMapper()
    dfs = {metric: pull_jhu_data(base_url, metric, gmpr) for metric in METRICS}
    for metric, geo_res, sensor, smoother in product(
        METRICS, GEO_RESOLUTIONS, SENSORS, SMOOTHERS
    ):
        if "cumulative" in sensor and "seven_day_average" in smoother:
            continue
        logger.info(
            event="generating signal and exporting to CSV",
            metric=metric,
            geo_res=geo_res,
            sensor=sensor,
            smoother=smoother)
        df = dfs[metric]
        # Aggregate to appropriate geographic resolution
        df = geo_map(df, geo_res, sensor)
        df.set_index(["timestamp", "geo_id"], inplace=True)
        df["val"] = df[sensor].groupby(level=1).transform(SMOOTHERS_MAP[smoother][0])
        df["se"] = np.nan
        df["sample_size"] = np.nan
        # Drop early entries where data insufficient for smoothing
        df = df[~df["val"].isnull()]
        df = df.reset_index()
        sensor_name = SENSOR_NAME_MAP[sensor][0]
        # if (SENSOR_NAME_MAP[sensor][1] or SMOOTHERS_MAP[smoother][2]):
        #     metric = f"wip_{metric}"
        #     sensor_name = WIP_SENSOR_NAME_MAP[sensor][0]
        sensor_name = SMOOTHERS_MAP[smoother][1] + sensor_name
        exported_csv_dates = create_export_csv(
            df,
            export_dir=export_dir,
            start_date=datetime.strptime(export_start_date, "%Y-%m-%d"),
            metric=metric,
            geo_res=geo_res,
            sensor=sensor_name,
        )
        if not exported_csv_dates.empty:
            csv_export_count += exported_csv_dates.size
            if not oldest_final_export_date:
                oldest_final_export_date = max(exported_csv_dates)
            oldest_final_export_date = min(
                oldest_final_export_date, max(exported_csv_dates))

    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    max_lag_in_days = None
    formatted_oldest_final_export_date = None
    if oldest_final_export_date:
        max_lag_in_days = (datetime.now() - oldest_final_export_date).days
        formatted_oldest_final_export_date = oldest_final_export_date.strftime("%Y-%m-%d")
    logger.info("Completed indicator run",
        elapsed_time_in_seconds = elapsed_time_in_seconds,
        csv_export_count = csv_export_count,
        max_lag_in_days = max_lag_in_days,
        oldest_final_export_date = formatted_oldest_final_export_date)
