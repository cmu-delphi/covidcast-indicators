# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.
"""
from datetime import datetime, date, time, timedelta
from itertools import product
from functools import partial
from os.path import join

import numpy as np
import pandas as pd
from delphi_utils import read_params, create_export_csv

from .geo import geo_map
from .pull import pull_usafacts_data
from .smooth import (
    identity,
    kday_moving_average,
)


# global constants
seven_day_moving_average = partial(kday_moving_average, k=7)
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
    "unsmoothed":           (identity, '', False, lambda d: d - timedelta(days=7)),
    "seven_day_average":    (seven_day_moving_average, '7dav_', True, lambda d: d),
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
    if export_start_date == "latest":
        export_start_date = datetime.combine(date.today(),time(0,0)) - timedelta(days=1)
    else:
        export_start_date = datetime.strptime(export_start_date, "%Y-%m-%d")
    export_dir = params["export_dir"]
    base_url = params["base_url"]
    static_file_dir = params["static_file_dir"]

    map_df = pd.read_csv(
        join(static_file_dir, "fips_prop_pop.csv"), dtype={"fips": int}
    )
    pop_df = pd.read_csv(
        join(static_file_dir, "fips_population.csv"),
        dtype={"fips": float, "population": float},
    ).rename({"fips": "FIPS"}, axis=1)

    dfs = {metric: pull_usafacts_data(base_url, metric, pop_df) for metric in METRICS}
    for metric, geo_res, sensor, smoother in product(
            METRICS, GEO_RESOLUTIONS, SENSORS, SMOOTHERS):
        print(geo_res, metric, sensor, smoother)
        df = dfs[metric]
        # Aggregate to appropriate geographic resolution
        df = geo_map(df, geo_res, map_df, sensor)
        df["val"] = SMOOTHERS_MAP[smoother][0](df[sensor].values)
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
