# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.
"""
from datetime import datetime, date, time, timedelta
from itertools import product
from os.path import join

import numpy as np
import pandas as pd
from delphi_utils import (
    create_export_csv,
    read_params,
    GeoMapper,
    S3ArchiveDiffer,
    Smoother
)

from .geo import geo_map
from .pull import pull_usafacts_data

# global constants
METRICS = [
    "confirmed",
    "deaths",
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
    "new_counts":           ("incidence_num", False),
    "cumulative_counts":    ("cumulative_num", False),
    "incidence":            ("incidence_prop", False),
    "cumulative_prop":      ("cumulative_prop", False),
}
# Temporarily added for wip_ signals
# WIP_SENSOR_NAME_MAP = {
#     "new_counts":           ("incid_num", False),
#     "cumulative_counts":    ("cumul_num", False),
#     "incidence":            ("incid_prop", False),
#     "cumulative_prop":      ("cumul_prop", False),
# }

SMOOTHERS_MAP = {
    "unsmoothed": (Smoother("identity"), "", False, lambda d: d - timedelta(days=7)),
    "seven_day_average": (Smoother("moving_average", window_length=7), "7dav_", True, lambda d: d),
}
GEO_RESOLUTIONS = [
    "county",
    "state",
    "msa",
    "hrr",
]


def run_module():
    """Run the usafacts indicator."""
    params = read_params()
    export_start_date = params["export_start_date"]
    if export_start_date == "latest":
        export_start_date = datetime.combine(date.today(), time(0, 0)) - timedelta(days=1)
    else:
        export_start_date = datetime.strptime(export_start_date, "%Y-%m-%d")
    export_dir = params["export_dir"]
    base_url = params["base_url"]
    cache_dir = params["cache_dir"]

    arch_diff = S3ArchiveDiffer(
        cache_dir, export_dir,
        params["bucket_name"], "usafacts",
        params["aws_credentials"])
    arch_diff.update_cache()

    geo_mapper = GeoMapper()

    dfs = {metric: pull_usafacts_data(base_url, metric, geo_mapper) for metric in METRICS}
    for metric, geo_res, sensor, smoother in product(
            METRICS, GEO_RESOLUTIONS, SENSORS, SMOOTHERS):
        print(geo_res, metric, sensor, smoother)
        df = dfs[metric]
        # Aggregate to appropriate geographic resolution
        df = geo_map(df, geo_res, sensor)
        df["val"] = SMOOTHERS_MAP[smoother][0].smooth(df[sensor].values)
        df["se"] = np.nan
        df["sample_size"] = np.nan
        # Drop early entries where data insufficient for smoothing
        df = df.loc[~df["val"].isnull(), :]
        sensor_name = SENSOR_NAME_MAP[sensor][0]
        # if (SENSOR_NAME_MAP[sensor][1] or SMOOTHERS_MAP[smoother][2]):
        #     metric = f"wip_{metric}"
        #     sensor_name = WIP_SENSOR_NAME_MAP[sensor][0]
        sensor_name = SMOOTHERS_MAP[smoother][1] + sensor_name
        create_export_csv(
            df,
            export_dir=export_dir,
            start_date=SMOOTHERS_MAP[smoother][3](export_start_date),
            metric=metric,
            geo_res=geo_res,
            sensor=sensor_name,
        )

    # Diff exports, and make incremental versions
    _, common_diffs, new_files = arch_diff.diff_exports()

    # Archive changed and new files only
    to_archive = [f for f, diff in common_diffs.items() if diff is not None]
    to_archive += new_files
    _, fails = arch_diff.archive_exports(to_archive)

    # Filter existing exports to exclude those that failed to archive
    succ_common_diffs = {f: diff for f, diff in common_diffs.items() if f not in fails}
    arch_diff.filter_exports(succ_common_diffs)

    # Report failures: someone should probably look at them
    for exported_file in fails:
        print(f"Failed to archive '{exported_file}'")
