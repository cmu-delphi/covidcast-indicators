# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.
"""
from datetime import datetime, date, timedelta
from itertools import product
from os.path import join

import numpy as np
import pandas as pd
from delphi_utils import read_params

from .pull import pull_nchs_mortality_data
from .export import export_csv

# global constants
METRICS = [
        'covid_deaths', 'total_deaths', 'pneumonia_deaths',
        'pneumonia_and_covid_deaths', 'influenza_deaths',
        'pneumonia_influenza_or_covid_19_deaths'
]
SENSORS = [
        "num",
        "prop"
]
INCIDENCE_BASE = 100000
geo_res = "state"

def run_module():

    params = read_params()
    export_start_date = params["export_start_date"]
    if export_start_date == "latest": # Find the previous Saturday
        export_start_date = date.today() - timedelta(
                days=date.today().weekday() + 2)
        export_start_date = export_start_date.strftime('%Y-%m-%d')
    export_dir = params["export_dir"]
    static_file_dir = params["static_file_dir"]
    token = params["token"]

    map_df = pd.read_csv(
        join(static_file_dir, "state_pop.csv"), dtype={"fips": int}
    )

    df = pull_nchs_mortality_data(token, map_df)
    for metric, sensor in product(METRICS, SENSORS):
        print(metric, sensor)
        if sensor == "num":
            df["val"] = df[metric]
        else:
            df["val"] = df[metric] / df["population"] * INCIDENCE_BASE
        df["se"] = np.nan
        df["sample_size"] = np.nan
        sensor_name = "_".join(["wip", metric, sensor])
        export_csv(
            df,
            geo_name=geo_res,
            export_dir=export_dir,
            start_date=datetime.strptime(export_start_date, "%Y-%m-%d"),
            sensor=sensor_name,
        )
