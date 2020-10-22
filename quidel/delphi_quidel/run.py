# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_quidel`.
"""
from delphi_utils import read_params

from .geo_maps import geo_map
from .pull import (pull_quidel_data,
                   check_export_start_date,
                   check_export_end_date,
                   update_cache_file)
from .export import export_csv
from .generate_sensor import (generate_sensor_for_states,
                              generate_sensor_for_other_geores)
from .constants import (END_FROM_TODAY_MINUS, EXPORT_DAY_RANGE,
                        GEO_RESOLUTIONS, SENSORS)
from .handle_wip_sensor import add_prefix

def run_module():
    params = read_params()
    cache_dir = params["cache_dir"]
    export_dir = params["export_dir"]
    export_start_dates = params["export_start_date"]
    export_end_dates = params["export_end_date"]

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

    for test_type in ["covid_ag", "flu_ag"]:
        print("For %s:"%test_type)
        data = dfs[test_type].copy()

        # For State Level
        state_groups = geo_map("state", data).groupby("state_id")
        first_date, last_date = data["timestamp"].min(), data["timestamp"].max()
        for sensor in sensors:
            if test_type not in sensor:
                continue
            print("state", sensor)
            state_df = generate_sensor_for_states(
                state_groups, smooth=SENSORS[sensor][1],
                device=SENSORS[sensor][0], first_date=first_date,
                last_date=last_date)
            export_csv(state_df, "state", sensor, receiving_dir=export_dir,
                       start_date=export_start_dates[test_type],
                       end_date=export_end_dates[test_type])

        # County/HRR/MSA level
        for geo_res in GEO_RESOLUTIONS:
            geo_data, res_key = geo_map(geo_res, data)
            for sensor in sensors:
                if test_type not in sensor:
                    continue
                print(geo_res, sensor)
                res_df = generate_sensor_for_other_geores(
                    state_groups, geo_data, res_key, smooth=SENSORS[sensor][1],
                    device=SENSORS[sensor][0], first_date=first_date,
                    last_date=last_date)
                export_csv(res_df, geo_res, sensor, receiving_dir=export_dir,
                           start_date=export_start_dates[test_type],
                           end_date=export_end_dates[test_type])

    # Export the cache file if the pipeline runs successfully.
    # Otherwise, don't update the cache file
    update_cache_file(dfs, _end_date, cache_dir)
