# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.
"""
from os.path import join

import pandas as pd
from delphi_utils import read_params, add_prefix, create_export_csv

from .geo_maps import geo_map
from .pull import (pull_quidel_data,
                   check_export_start_date,
                   check_export_end_date,
                   update_cache_file)
from .generate_sensor import (generate_sensor_for_states,
                              generate_sensor_for_other_geores)
from .constants import (END_FROM_TODAY_MINUS, EXPORT_DAY_RANGE,
                        GEO_RESOLUTIONS, SENSORS)

def run_module():
    """Run Quidel flu test module."""
    params = read_params()
    cache_dir = params["cache_dir"]
    export_dir = params["export_dir"]
    static_file_dir = params["static_file_dir"]
    export_start_dates = params["export_start_date"]
    export_end_dates = params["export_end_date"]
    map_df = pd.read_csv(
        join(static_file_dir, "fips_prop_pop.csv"), dtype={"fips": int}
    )

    # Pull data and update export date
    dfs, _end_date = pull_quidel_data(params)
    if _end_date is None:
        print("The data is up-to-date. Currently, no new data to be ingested.")
        return
    export_end_dates = check_export_end_date(export_end_dates, _end_date,
                                             END_FROM_TODAY_MINUS)
    export_start_dates = check_export_start_date(export_start_dates,
                                                 export_end_dates,
                                                 EXPORT_DAY_RANGE)

    # Add prefix, if required
    sensors = add_prefix(list(SENSORS.keys()),
                         wip_signal=params["wip_signal"],
                         prefix="wip_")

    for sensor in sensors:
        # Check either covid_ag or flu_ag
        test_type = "covid_ag" if "covid_ag" in sensor else "flu_ag"
        print("state", sensor)
        data = dfs[test_type].copy()
        state_groups = geo_map("state", data, map_df).groupby("state_id")
        first_date, last_date = data["timestamp"].min(), data["timestamp"].max()

        # For State Level
        state_df = generate_sensor_for_states(
            state_groups, smooth=SENSORS[sensor][1],
            device=SENSORS[sensor][0], first_date=first_date,
            last_date=last_date)
        create_export_csv(state_df, geo_res="state", sensor=sensor, export_dir=export_dir,
                          start_date=export_start_dates[test_type],
                          end_date=export_end_dates[test_type])

        # County/HRR/MSA level
        for geo_res in GEO_RESOLUTIONS:
            print(geo_res, sensor)
            data = dfs[test_type].copy()
            data, res_key = geo_map(geo_res, data, map_df)
            res_df = generate_sensor_for_other_geores(
                state_groups, data, res_key, smooth=SENSORS[sensor][1],
                device=SENSORS[sensor][0], first_date=first_date,
                last_date=last_date)
            create_export_csv(res_df, geo_res=geo_res, sensor=sensor, export_dir=export_dir,
                              start_date=export_start_dates[test_type],
                              end_date=export_end_dates[test_type],
                              remove_null_samples=True)

    # Export the cache file if the pipeline runs successfully.
    # Otherwise, don't update the cache file
    update_cache_file(dfs, _end_date, cache_dir)
