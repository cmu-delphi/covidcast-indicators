# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.
"""
from datetime import datetime
from itertools import product
from functools import partial
from os.path import join

import numpy as np
import pandas as pd
from delphi_utils import (
    read_params,
    create_export_csv,
    S3ArchiveDiffer,
)

from .geo import geo_map
from .pull import pull_jhu_data
from .smooth import (
    identity,
    kday_moving_average,
)


# global constants
seven_day_moving_average = partial(kday_moving_average, k=7)
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
    "unsmoothed":           (identity, ''),
    "seven_day_average":    (seven_day_moving_average, '7dav_'),
}
GEO_RESOLUTIONS = [
    "county",
    "state",
    "msa",
    "hrr",
]


def run_module():

    params = read_params()
    export_start_date = params["export_start_date"]
    export_dir = params["export_dir"]
    base_url = params["base_url"]
    static_file_dir = params["static_file_dir"]
    cache_dir = params["cache_dir"]

    arch_diff = S3ArchiveDiffer(
        cache_dir, export_dir,
        params["bucket_name"], "jhu",
        params["aws_credentials"])
    arch_diff.update_cache()

    pop_df = pd.read_csv(
        join(static_file_dir, "fips_population.csv"),
        dtype={"fips": float, "population": float},
    )

    dfs = {metric: pull_jhu_data(base_url, metric, pop_df) for metric in METRICS}
    for metric, geo_res, sensor, smoother in product(
            METRICS, GEO_RESOLUTIONS, SENSORS, SMOOTHERS):
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
