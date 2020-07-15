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

from .geo_maps import GeoMaps
from .pull import pull_quidel_covidtest
from .data_tools import *
from .export import *
from .generate_sensor import generate_sensor_for_states, generate_sensor_for_other_geores

# global constants
MIN_OBS = 50  # minimum number of observations in order to compute a proportion.
POOL_DAYS = 7  # number of days in the past (including today) to pool over

GEO_RESOLUTIONS = [
    "county",
    "msa",
    "hrr"
]
SENSORS = [
    "smoothed_pct_positive",
    "raw_pct_positive"
]
SMOOTHERS = {
    "smoothed_pct_positive": True,
    "raw_pct_positive": False
}


def run_module():

    params = read_params()
    export_start_date = datetime.strptime(params["export_start_date"], '%Y-%m-%d')
    if params["export_end_date"] == "":
        export_end_date = datetime.today() - timedelta(days=5)
    else:
        export_end_date = datetime.strptime(params["export_end_date"], '%Y-%m-%d')
        
    export_dir = params["export_dir"]
    pull_start_date = datetime.strptime(params["pull_start_date"], '%Y-%m-%d').date()
    static_file_dir = params["static_file_dir"]
    
    mail_server = params["mail_server"]
    account = params["account"]
    password = params["password"]
    sender = params["sender"]
    
    pull_end_date = date.today()

    map_df = pd.read_csv(
        join(static_file_dir, "fips_prop_pop.csv"), dtype={"fips": int}
    )

    pop_df = pd.read_csv(
        join(static_file_dir, "fips_population.csv"),
        dtype={"fips": float, "population": float},
    ).rename({"fips": "FIPS"}, axis=1)

    df = pull_quidel_covidtest(pull_start_date, pull_end_date, mail_server,
                               account, sender, password) 
    geo_map = GeoMaps()
        
    # State Level
    data = df.copy()
    state_data  = geo_map.zip_to_state(data, map_df)
    
    raw_state_df, state_groups = generate_sensor_for_states(state_data, smooth = False)
    export_csv(raw_state_df, "state", "raw_pct_positive", receiving_dir=export_dir,
               start_date = export_start_date, end_date = export_end_date)  
    
    smoothed_state_df, _ = generate_sensor_for_states(state_data, smooth = True)
    export_csv(smoothed_state_df, "state", "smoothed_pct_positive", receiving_dir=export_dir,
               start_date = export_start_date, end_date = export_end_date) 
    
    for geo_res, sensor in product(GEO_RESOLUTIONS, SENSORS):
        print(geo_res, sensor)
        data = df.copy()
        if geo_res == "county":
            data, res_key = geo_map.zip_to_county(data, map_df)
            res_groups = data.groupby(res_key)
        elif geo_res == "msa":
            data, res_key = geo_map.zip_to_msa(data, map_df)
            res_groups = data.groupby(res_key)
        else:
            data, res_key = geo_map.zip_to_hrr(data, map_df)
        
        res_df = generate_sensor_for_other_geores(state_groups, data, res_key, 
                                                  smooth = SMOOTHERS[sensor])
        export_csv(res_df, geo_res, sensor, receiving_dir=export_dir,
                   start_date = export_start_date, end_date = export_end_date)
        
    return
        
        
