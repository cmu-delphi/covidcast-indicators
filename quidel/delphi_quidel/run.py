# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.
"""
import time
from os.path import join
from typing import Dict, Any

import pandas as pd
from delphi_utils import (
    add_prefix,
    create_export_csv,
    get_structured_logger
)

from .constants import (END_FROM_TODAY_MINUS, EXPORT_DAY_RANGE,
                        GEO_RESOLUTIONS, SENSORS)
from .generate_sensor import (generate_sensor_for_states,
                              generate_sensor_for_other_geores)
from .geo_maps import geo_map
from .pull import (pull_quidel_data,
                   check_export_start_date,
                   check_export_end_date,
                   update_cache_file)


def run_module(params: Dict[str, Any]):
    """Run Quidel flu test module.

    The `params` argument is expected to have the following structure:
    - "common":
        - "export_dir": str, directory to write output
        - "log_exceptions" (optional): bool, whether to log exceptions to file
        - "log_filename" (optional): str, name of file to write logs
    - indicator":
        - "static_file_dir": str, directory name with population information
        - "input_cache_dir": str, directory in which to cache input data
        - "export_start_date": str, YYYY-MM-DD format of earliest date to create output
        - "export_end_date": str, YYYY-MM-DD format of latest date to create output or "" to create
                             through the present
        - "pull_start_date": str, YYYY-MM-DD format of earliest date to pull input
        - "pull_end_date": str, YYYY-MM-DD format of latest date to create output or "" to create
                           through the present
        - "aws_credentials": Dict[str, str], authentication parameters for AWS S3; see S3
                             documentation
        - "bucket_name": str, name of AWS bucket in which to find data
        - "wip_signal": List[str], list of signal names that are works in progress
        - "test_mode": bool, whether we are running in test mode
    """
    start_time = time.time()
    logger = get_structured_logger(
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))
    cache_dir = params["indicator"]["input_cache_dir"]
    export_dir = params["common"]["export_dir"]
    static_file_dir = params["indicator"]["static_file_dir"]
    export_start_dates = params["indicator"]["export_start_date"]
    export_end_dates = params["indicator"]["export_end_date"]
    map_df = pd.read_csv(
        join(static_file_dir, "fips_prop_pop.csv"), dtype={"fips": int}
    )

    # Pull data and update export date
    dfs, _end_date = pull_quidel_data(params["indicator"])
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
                         wip_signal=params["indicator"]["wip_signal"],
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

    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    logger.info("Completed indicator run",
        elapsed_time_in_seconds = elapsed_time_in_seconds)
