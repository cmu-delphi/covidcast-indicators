# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.
"""
from os.path import join

import pandas as pd
from delphi_utils import read_params

from .geo_maps import (zip_to_msa, zip_to_hrr, zip_to_county, zip_to_state)
from .pull import (pull_quidel_covidtest,
                   check_export_start_date,
                   check_export_end_date,
                   update_cache_file)
from .export import export_csv
from .generate_sensor import (generate_sensor_for_states,
                              generate_sensor_for_other_geores)
from .constants import (END_FROM_TODAY_MINUS, EXPORT_DAY_RANGE,
                        SMOOTHED_POSITIVE, RAW_POSITIVE,
                        SMOOTHED_TEST_PER_DEVICE, RAW_TEST_PER_DEVICE,
                        GEO_RESOLUTIONS, SENSORS, SMOOTHERS,
                        COUNTY, MSA)
from .handle_wip_sensor import add_prefix


def run_module():
    params = read_params()
    cache_dir = params["cache_dir"]
    export_dir = params["export_dir"]
    static_file_dir = params["static_file_dir"]
    export_start_date = params["export_start_date"]
    export_end_date = params["export_end_date"]
    map_df = pd.read_csv(
        join(static_file_dir, "fips_prop_pop.csv"), dtype={"fips": int}
    )

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
    state_groups = zip_to_state(data, map_df).groupby("state_id")

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
        export_csv(state_df, "state", sensor, receiving_dir=export_dir,
                   start_date=export_start_date, end_date=export_end_date)

        # County/HRR/MSA level
        for geo_res in GEO_RESOLUTIONS:
            print(geo_res, sensor)
            data = df.copy()
            if geo_res == COUNTY:
                data, res_key = zip_to_county(data, map_df)
            elif geo_res == MSA:
                data, res_key = zip_to_msa(data, map_df)
            else:
                data, res_key = zip_to_hrr(data, map_df)

            res_df = generate_sensor_for_other_geores(
                state_groups, data, res_key, smooth=smoothers[sensor][1],
                device=smoothers[sensor][0], first_date=first_date,
                last_date=last_date)
            export_csv(res_df, geo_res, sensor, receiving_dir=export_dir,
                       start_date=export_start_date, end_date=export_end_date)

    # Export the cache file if the pipeline runs successfully.
    # Otherwise, don't update the cache file
    update_cache_file(df, _end_date, cache_dir)
