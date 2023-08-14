# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.
"""
import atexit
from datetime import datetime
from multiprocessing import Pool, cpu_count, current_process
import time
from typing import Dict, Any

from delphi_utils import (
    add_prefix,
    create_export_csv,
    get_structured_logger
)
from delphi_utils.logger import pool_and_threadedlogger

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
from .backfill import (store_backfill_file, merge_backfill_file)

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
    """Get smooth info from SMOOTHERS."""
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

def generate_and_export_for_nonparent_geo(geo_groups, res_key, smooth, device,
                                          first_date, last_date, suffix, # generate args
                                          geo_res, sensor_name, export_dir,
                                          export_start_date, export_end_date, # export args
                                          threaded_logger): # logger args
    """Generate sensors, create export CSV then return stats."""
    threaded_logger.info("Generating signal and exporting to CSV",
                         geo_res=geo_res,
                         sensor=sensor_name,
                         pid=current_process().pid)
    res_df = generate_sensor_for_nonparent_geo(geo_groups, res_key, smooth, device,
                                               first_date, last_date, suffix)
    dates = create_export_csv(res_df, geo_res=geo_res,
                              sensor=sensor_name, export_dir=export_dir,
                              start_date=export_start_date,
                              end_date=export_end_date)
    return dates

def generate_and_export_for_parent_geo(geo_groups, geo_data, res_key, smooth, device,
                                       first_date, last_date, suffix, # generate args
                                       geo_res, sensor_name, export_dir,
                                       export_start_date, export_end_date, # export args
                                       threaded_logger): # logger args
    """Generate sensors, create export CSV then return stats."""
    threaded_logger.info("Generating signal and exporting to CSV",
                         geo_res=geo_res,
                         sensor=sensor_name,
                         pid=current_process().pid)
    res_df = generate_sensor_for_parent_geo(geo_groups, geo_data, res_key, smooth, device,
                                            first_date, last_date, suffix)
    dates = create_export_csv(res_df, geo_res=geo_res,
                              sensor=sensor_name, export_dir=export_dir,
                              start_date=export_start_date,
                              end_date=export_end_date,
                              remove_null_samples=True) # for parent geo, remove null sample size
    return dates

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
        - "backfill_dir": str, directory in which to store the backfill files
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

    # Allow user to turn backfill file generation on or off. Defaults to True
    # (generate files).
    if params["indicator"].get("generate_backfill_files", True):
        backfill_dir = params["indicator"]["backfill_dir"]
        backfill_merge_day = params["indicator"]["backfill_merge_day"]

        # Merge 4 weeks' data into one file to save runtime
        # Notice that here we don't check the _end_date(receive date)
        # since we always want such merging happens on a certain day of a week
        merge_backfill_file(backfill_dir, backfill_merge_day, datetime.today())
        if _end_date is None:
            logger.info("The data is up-to-date. Currently, no new data to be ingested.")
            return
        # Store the backfill intermediate file
        store_backfill_file(df, _end_date, backfill_dir)

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
    n_cpu = min(8, cpu_count()) # for parallelization
    with pool_and_threadedlogger(logger, n_cpu) as (pool, threaded_logger):
        # for using loggers in multiple threads
        # disabled due to a Pylint bug, resolved by version bump (#1886)
        logger.info("Parallelizing sensor generation", n_cpu=n_cpu)
        with Pool(n_cpu) as pool:
            pool_results = []
            for geo_res in NONPARENT_GEO_RESOLUTIONS:
                geo_data, res_key = geo_map(geo_res, data)
                geo_groups = geo_data.groupby(res_key)
                for agegroup in AGE_GROUPS:
                    for sensor in sensors:
                        if agegroup == "total":
                            sensor_name = sensor
                        else:
                            sensor_name = "_".join([sensor, agegroup])
                        pool_results.append(
                            pool.apply_async(
                                generate_and_export_for_nonparent_geo,
                                args=(
                                    # generate_sensors_for_parent_geo
                                    geo_groups, res_key,
                                    smoothers[sensor][1], smoothers[sensor][0],
                                    first_date, last_date, agegroup,
                                    # create_export_csv
                                    geo_res, sensor_name, export_dir,
                                    export_start_date, export_end_date,
                                    # logger
                                    threaded_logger
                                )
                            )
                        )
            assert geo_res == "state" # Make sure geo_groups is for state level
            # County/HRR/MSA level
            for geo_res in PARENT_GEO_RESOLUTIONS:
                geo_data, res_key = geo_map(geo_res, data) # using the last geo_groups
                for agegroup in AGE_GROUPS:
                    for sensor in sensors:
                        if agegroup == "total":
                            sensor_name = sensor
                        else:
                            sensor_name = "_".join([sensor, agegroup])
                        pool_results.append(
                            pool.apply_async(
                                generate_and_export_for_parent_geo,
                                args=(
                                    # generate_sensors_for_parent_geo
                                    geo_groups, geo_data, res_key,
                                    smoothers[sensor][1], smoothers[sensor][0],
                                    first_date, last_date, agegroup,
                                    # create_export_csv
                                    geo_res, sensor_name, export_dir,
                                    export_start_date, export_end_date,
                                    # logger
                                    threaded_logger
                                )
                            )
                        )
            pool_results = [proc.get() for proc in pool_results]
            for dates in pool_results:
                if len(dates) > 0:
                    stats.append((max(dates), len(dates)))

    # Export the cache file if the pipeline runs successfully.
    # Otherwise, don't update the cache file
    update_cache_file(df, _end_date, cache_dir)
