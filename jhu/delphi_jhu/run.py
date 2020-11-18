# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.
"""
from datetime import datetime
from itertools import product

import numpy as np
from delphi_utils import (
    read_params,
    create_export_csv,
    S3ArchiveDiffer,
    Smoother,
    GeoMapper,
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
]


def run_module():
    """Run the JHU indicator module."""
    params = read_params()
    export_start_date = params["export_start_date"]
    export_dir = params["export_dir"]
    base_url = params["base_url"]
    cache_dir = params["cache_dir"]

    if len(params["bucket_name"]) > 0:
        arch_diff = S3ArchiveDiffer(
            cache_dir,
            export_dir,
            params["bucket_name"],
            "jhu",
            params["aws_credentials"],
        )
        arch_diff.update_cache()
    else:
        arch_diff = None

    gmpr = GeoMapper()
    dfs = {metric: pull_jhu_data(base_url, metric, gmpr) for metric in METRICS}
    for metric, geo_res, sensor, smoother in product(
        METRICS, GEO_RESOLUTIONS, SENSORS, SMOOTHERS
    ):
        print(metric, geo_res, sensor, smoother)
        df = dfs[metric]
        # Aggregate to appropriate geographic resolution
        df = geo_map(df, geo_res)
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
        create_export_csv(
            df,
            export_dir=export_dir,
            start_date=datetime.strptime(export_start_date, "%Y-%m-%d"),
            metric=metric,
            geo_res=geo_res,
            sensor=sensor_name,
        )

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
