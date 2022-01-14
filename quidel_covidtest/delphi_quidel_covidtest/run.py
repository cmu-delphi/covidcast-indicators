# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.
"""
import atexit
from datetime import datetime
import time
from typing import Dict, Any

from delphi_utils import (
    add_prefix,
    create_export_csv,
    get_structured_logger
)

from .constants import (END_FROM_TODAY_MINUS,
                        SMOOTHED_POSITIVE, RAW_POSITIVE,
                        SMOOTHED_TEST_PER_DEVICE, RAW_TEST_PER_DEVICE,
                        PARENT_GEO_RESOLUTIONS, SENSORS, SMOOTHERS, NONPARENT_GEO_RESOLUTIONS,
                        AGE_GROUPS)
from .generate_sensor import generate_sensor_for_parent_geo, generate_sensor_for_nonparent_geo
from .geo_maps import geo_map
from .pull import (pull_quidel_covidtest,
                   check_export_start_date,
                   check_export_end_date,
                   update_cache_file)


def log_exit(start_time, stats, logger):
    """Log at program exit."""
    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    min_max_date = stats and min(s[0] for s in stats)
    csv_export_count = sum(s[-1] for s in stats)
    max_lag_in_days = min_max_date and (datetime.now() - min_max_date).days
    formatted_min_max_date = min_max_date and min_max_date.strftime("%Y-%m-%d")
    logger.info("Completed indicator run",
                elapsed_time_in_seconds = elapsed_time_in_seconds,
                csv_export_count = csv_export_count,
                max_lag_in_days = max_lag_in_days,
                oldest_final_export_date = formatted_min_max_date)

def get_smooth_info(sensors, _SMOOTHERS):
    """Get smooth info from SMOOTHERS. """
    smoothers = _SMOOTHERS.copy()
    for sensor in sensors:
        if sensor.endswith(SMOOTHED_POSITIVE):
            smoothers[sensor] = smoothers.pop(SMOOTHED_POSITIVE)
        elif sensor.endswith(RAW_POSITIVE):
            smoothers[sensor] = smoothers.pop(RAW_POSITIVE)
        elif sensor.endswith(SMOOTHED_TEST_PER_DEVICE):
            smoothers[sensor] = smoothers.pop(SMOOTHED_TEST_PER_DEVICE)
        else:
            smoothers[sensor] = smoothers.pop(RAW_TEST_PER_DEVICE)
    return smoothers

def run_module(params: Dict[str, Any]):
    """Run the quidel_covidtest indicator.

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
    stats = []
    atexit.register(log_exit, start_time, stats, logger)
    cache_dir = params["indicator"]["input_cache_dir"]
    export_dir = params["common"]["export_dir"]
    export_start_date = params["indicator"]["export_start_date"]
    export_end_date = params["indicator"]["export_end_date"]
    export_day_range = params["indicator"]["export_day_range"]

    # Pull data and update export date
    df, _end_date = pull_quidel_covidtest(params["indicator"], logger)
    if _end_date is None:
        logger.info("The data is up-to-date. Currently, no new data to be ingested.")
        return
    export_end_date = check_export_end_date(
        export_end_date, _end_date, END_FROM_TODAY_MINUS)
    export_start_date = check_export_start_date(
        export_start_date, export_end_date, export_day_range)

    first_date, last_date = df["timestamp"].min(), df["timestamp"].max()

    data = df.copy()
    # Add prefix, if required
    sensors = add_prefix(SENSORS,
                         wip_signal=params["indicator"]["wip_signal"],
                         prefix="wip_")
    smoothers = get_smooth_info(sensors, SMOOTHERS)
    for geo_res in NONPARENT_GEO_RESOLUTIONS:
        geo_data, res_key = geo_map(geo_res, data)
        geo_groups = geo_data.groupby(res_key)
        for agegroup in AGE_GROUPS:
            for sensor in sensors:
                if agegroup == "total":
                    sensor_name = sensor
                else:
                    sensor_name = "_".join([sensor, agegroup])
                logger.info("Generating signal and exporting to CSV",
                            geo_res=geo_res,
                            sensor=sensor_name)
                state_df = generate_sensor_for_nonparent_geo(
                    geo_groups, res_key, smooth=smoothers[sensor][1],
                    device=smoothers[sensor][0], first_date=first_date,
                    last_date=last_date, suffix=agegroup)
                dates = create_export_csv(
                    state_df,
                    geo_res=geo_res,
                    sensor=sensor_name,
                    export_dir=export_dir,
                    start_date=export_start_date,
                    end_date=export_end_date)
                if len(dates) > 0:
                    stats.append((max(dates), len(dates)))

    # County/HRR/MSA level
    for geo_res in PARENT_GEO_RESOLUTIONS:
        geo_data, res_key = geo_map(geo_res, data)
        for agegroup in AGE_GROUPS:
            for sensor in sensors:
                if agegroup == "total":
                    sensor_name = sensor
                else:
                    sensor_name = "_".join([sensor, agegroup])
                logger.info("Generating signal and exporting to CSV",
                            geo_res=geo_res,
                            sensor=sensor_name)
                res_df = generate_sensor_for_parent_geo(
                    geo_groups, geo_data, res_key, smooth=smoothers[sensor][1],
                    device=smoothers[sensor][0], first_date=first_date,
                    last_date=last_date, suffix=agegroup)
                dates = create_export_csv(res_df, geo_res=geo_res,
                                          sensor=sensor_name, export_dir=export_dir,
                                          start_date=export_start_date,
                                          end_date=export_end_date,
                                          remove_null_samples=True)
                if len(dates) > 0:
                    stats.append((max(dates), len(dates)))

    # Export the cache file if the pipeline runs successfully.
    # Otherwise, don't update the cache file
    update_cache_file(df, _end_date, cache_dir)
