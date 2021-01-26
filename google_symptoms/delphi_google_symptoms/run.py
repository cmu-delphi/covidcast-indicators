# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_google_symptoms`.
"""
from datetime import datetime
from itertools import product

import numpy as np
from delphi_utils import read_params, create_export_csv, geomap

from .pull import pull_gs_data
from .geo import geo_map
from .constants import (METRICS, COMBINED_METRIC,
                        GEO_RESOLUTIONS, SMOOTHERS, SMOOTHERS_MAP)


def run_module():
    """Run Google Symptoms module."""
    params = read_params()
    export_start_date = datetime.strptime(
        params["export_start_date"], "%Y-%m-%d")
    export_dir = params["export_dir"]

    # Pull GS data
    dfs = pull_gs_data(params["path_to_bigquery_credentials"],
                       export_dir, export_start_date)
    gmpr = geomap.GeoMapper()
    for geo_res in GEO_RESOLUTIONS:
        if geo_res == "state":
            df_pull = dfs["state"]
        elif geo_res in ["hhs", "nation"]:
            df_pull = gmpr.replace_geocode(dfs["county"], "fips", geo_res, from_col="geo_id",
                                           date_col="timestamp")
            df_pull.rename(columns={geo_res: "geo_id"}, inplace=True)
        else:
            df_pull = geo_map(dfs["county"], geo_res)
        for metric, smoother in product(
                METRICS+[COMBINED_METRIC], SMOOTHERS):
            print(geo_res, metric, smoother)
            df = df_pull.set_index(["timestamp", "geo_id"])
            df["val"] = df[metric].groupby(level=1
                                           ).transform(SMOOTHERS_MAP[smoother][0])
            df["se"] = np.nan
            df["sample_size"] = np.nan
            # Drop early entries where data insufficient for smoothing
            df = df.loc[~df["val"].isnull(), :]
            df = df.reset_index()
            sensor_name = "_".join([smoother, "search"])
            create_export_csv(
                df,
                export_dir=export_dir,
                start_date=SMOOTHERS_MAP[smoother][1](export_start_date),
                metric=metric.lower(),
                geo_res=geo_res,
                sensor=sensor_name)
