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
from delphi_utils import read_params, create_export_csv

from .geo import geo_map
from .pull import pull_cds_data
from .smooth import (
    identity,
    kday_moving_average,
)


# global constants
MIN_OBS = 50 # minimum number of observations in order to compute a proportion.
seven_day_moving_average = partial(kday_moving_average, k=7)
METRICS = [
    "confirmed",
    "tested",
]
METRICS_LEVELS = {
    "confirmed": ["county"],
    "tested": ["county", "state"]
}
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
WIP_SENSOR_NAME_MAP = {
    "new_counts":           ("incid_num", False),
    "cumulative_counts":    ("cumul_num", False),
    "incidence":            ("incid_prop", False),
    "cumulative_prop":      ("cumul_prop", False),
}
SMOOTHERS_MAP = {
    "unsmoothed":           (identity, '', False),
    "seven_day_average":    (seven_day_moving_average, '7dav_', True),
}
GEO_RESOLUTIONS = [
    "county",
    "state",
    "msa",
    "hrr"
]
GEO_USELEVEL = {
    metric: {
        geo_res: "county" for geo_res in GEO_RESOLUTIONS
        } for metric in METRICS
}
# Treat state level as a special case for testing data because of the
# data coverage problem
GEO_USELEVEL["tested"]["state"] = "state"


def run_module():

    params = read_params()
    export_start_dates = {
            "confirmed": params["confirmed_export_start_date"],
            "tested": params["tested_export_start_date"]
    }
    export_dir = params["export_dir"]
    base_url = params["base_url"]
    static_file_dir = params["static_file_dir"]

    map_df = pd.read_csv(
        join(static_file_dir, "fips_prop_pop.csv"), dtype={"fips": int}
    )
    pop_df = pd.read_csv(
        join(static_file_dir, "fips_population.csv"),
        dtype={"fips": int, "population": float},
    )
    countyname_to_fips_df = pd.read_csv(
        join(static_file_dir, "countyname_to_fips.csv"), dtype={"fips": int}
    )[["fips", "name"]]
    statename_to_fakefips_df = pd.read_csv(
        join(static_file_dir, "statename_to_fakefips.csv")
    )
    PULL_MAPPING = {
        "county": countyname_to_fips_df,
        "state": statename_to_fakefips_df
    }

    pulled_df = {
                metric: {
                        level: pull_cds_data(
                            base_url, metric, level,
                            PULL_MAPPING,
                            pop_df
                            ) for level in METRICS_LEVELS[metric]
                        } for metric in METRICS
                }

    for geo_res, smoother in product(GEO_RESOLUTIONS, SMOOTHERS):
        # For tested and confirmed cases
        for sensor, metric in product(SENSORS, METRICS):
            print(geo_res, metric, sensor, smoother)
            used_geo_res = GEO_USELEVEL[metric][geo_res]
            # Aggregate to appropriate geographic resolution
            df = geo_map(pulled_df[metric][used_geo_res], geo_res, map_df)
            df["val"] = SMOOTHERS_MAP[smoother][0](df[sensor].values)
            if sensor == "new_counts":
                if metric == "tested":
                    tested_df = df.copy()
                else:
                    positive_df = df.copy()
            df["se"] = np.nan
            df["sample_size"] = np.nan
            # Drop early entries where data insufficient for smoothing
            df = df.loc[~df["val"].isnull(), :]
            sensor_name = SENSOR_NAME_MAP[sensor][0]
            if (SENSOR_NAME_MAP[sensor][1] or SMOOTHERS_MAP[smoother][2]):
                sensor_name = WIP_SENSOR_NAME_MAP[sensor][0]
            sensor_name = SMOOTHERS_MAP[smoother][1] + sensor_name
            create_export_csv(
                df,
                export_dir=export_dir,
                start_date=datetime.strptime(
                        export_start_dates[metric], "%Y-%m-%d"),
                metric="wip_" + metric,
                geo_res=geo_res,
                sensor=sensor_name,
            )

        # For positivity rate
        print(geo_res, "pct_positive", smoother)
        df = pd.merge(tested_df, positive_df,
                      on=["geo_id", "timestamp"],
                      suffixes=('_tested', '_confirmed'),
                      how="inner")[["geo_id", "timestamp",
                                      "val_tested", "val_confirmed"]]
        df = df.loc[
            (df["val_tested"] >= MIN_OBS) # threshold
            & (df["val_confirmed"] >= 0)
            & (df["val_confirmed"] <= df["val_tested"])
        ]
        df["val"] = df["val_confirmed"] / df["val_tested"] * 100
        df["sample_size"] = df["val_tested"]

        # Calculate Standard Error
        df.loc[df["sample_size"] == 0, "se"] = 0
        df.loc[df["sample_size"] > 0, "se"] = np.sqrt(
            df["val"]/100 * (1-df["val"]/100) / df["sample_size"]) * 100

        if smoother == "unsmoothed":
            metric = "wip_raw"
        else:
            metric = "wip_smoothed"
        sensor_name = SMOOTHERS_MAP[smoother][1] + sensor_name
        create_export_csv(
            df,
            export_dir=export_dir,
            start_date=datetime.strptime(
                    export_start_dates["tested"], "%Y-%m-%d"),
            metric=metric,
            geo_res=geo_res,
            sensor="pct_positive",
        )
  