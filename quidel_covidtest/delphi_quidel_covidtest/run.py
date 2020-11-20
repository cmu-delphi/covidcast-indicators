# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.
"""
from delphi_utils import read_params, add_prefix, create_export_csv

from .geo_maps import geo_map
from .pull import (pull_quidel_covidtest,
                   check_export_start_date,
                   check_export_end_date,
                   update_cache_file)
from .generate_sensor import (generate_sensor_for_states,
                              generate_sensor_for_other_geores)
from .constants import (END_FROM_TODAY_MINUS, EXPORT_DAY_RANGE,
                        SMOOTHED_POSITIVE, RAW_POSITIVE,
                        SMOOTHED_TEST_PER_DEVICE, RAW_TEST_PER_DEVICE,
                        GEO_RESOLUTIONS, SENSORS, SMOOTHERS)


def run_module():
    """Run the quidel_covidtest indicator."""
    params = read_params()
    cache_dir = params["cache_dir"]
    export_dir = params["export_dir"]
    export_start_date = params["export_start_date"]
    export_end_date = params["export_end_date"]

    # Pull data and update export date
    df, _end_date = pull_quidel_covidtest(params)
    if _end_date is None:
        print("The data is up-to-date. Currently, no new data to be ingested.")
        return
    export_end_date = check_export_end_date(export_end_date, _end_date,
                                            END_FROM_TODAY_MINUS)
    export_start_date = check_export_start_date(export_start_date,
                                                export_end_date, EXPORT_DAY_RANGE)

    first_date, last_date = df["timestamp"].min(), df["timestamp"].max()

    # State Level
    data = df.copy()
    state_groups = geo_map("state", data).groupby("state_id")

    # Add prefix, if required
    sensors = add_prefix(SENSORS,
                         wip_signal=read_params()["wip_signal"],
                         prefix="wip_")
    smoothers = SMOOTHERS.copy()

    for sensor in sensors:
        # For State Level
        print("state", sensor)
        if sensor.endswith(SMOOTHED_POSITIVE):
            smoothers[sensor] = smoothers.pop(SMOOTHED_POSITIVE)
        elif sensor.endswith(RAW_POSITIVE):
            smoothers[sensor] = smoothers.pop(RAW_POSITIVE)
        elif sensor.endswith(SMOOTHED_TEST_PER_DEVICE):
            smoothers[sensor] = smoothers.pop(SMOOTHED_TEST_PER_DEVICE)
        else:
            smoothers[sensor] = smoothers.pop(RAW_TEST_PER_DEVICE)
        state_df = generate_sensor_for_states(
            state_groups, smooth=smoothers[sensor][1],
            device=smoothers[sensor][0], first_date=first_date,
            last_date=last_date)
        create_export_csv(state_df, geo_res="state", sensor=sensor, export_dir=export_dir,
                          start_date=export_start_date, end_date=export_end_date)

    # County/HRR/MSA level
    for geo_res in GEO_RESOLUTIONS:
        geo_data, res_key = geo_map(geo_res, data)
        for sensor in sensors:
            print(geo_res, sensor)
            res_df = generate_sensor_for_other_geores(
                state_groups, geo_data, res_key, smooth=smoothers[sensor][1],
                device=smoothers[sensor][0], first_date=first_date,
                last_date=last_date)
            create_export_csv(res_df, geo_res=geo_res, sensor=sensor, export_dir=export_dir,
                              start_date=export_start_date, end_date=export_end_date,
                              remove_null_samples=True)


    # Export the cache file if the pipeline runs successfully.
    # Otherwise, don't update the cache file
    update_cache_file(df, _end_date, cache_dir)
