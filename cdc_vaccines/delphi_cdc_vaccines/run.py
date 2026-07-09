# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_cdc_vaccines`.
`run_module`'s lone argument should be a nested dictionary of
parameters loaded from the params.json file.
We expect the `params` to have the following structure:
    - "common":
        - "export_dir": str, directory to which the results are exported
        - "log_filename": (optional) str, path to log file
    - "indicator": (optional)
        - "wip_signal": (optional) Any[str, bool], list of signals that are works in progress, or
            True if all signals in the registry are works in progress, or False if only
            unpublished signals are.  See `delphi_utils.add_prefix()`
        - Any other indicator-specific settings
"""
from datetime import timedelta, datetime
from itertools import product
import time as tm
import os

from pandas import DataFrame

from delphi_utils.export import create_export_csv
from delphi_utils.geomap import GeoMapper
from delphi_utils import get_structured_logger
from delphi_utils.nancodes import Nans
from .constants import GEOS, SIGNALS, SMOOTHERS
from .pull import pull_cdcvacc_data


def add_nancodes(df: DataFrame) -> DataFrame:
    """
    Provide default nancodes for a non-survey indicator.

    Arguments
    --------
    params:  DataFrame
    """
    df["missing_val"] = Nans.NOT_MISSING
    df["missing_se"] = Nans.NOT_APPLICABLE
    df["missing_sample_size"] = Nans.NOT_APPLICABLE

    # Mark an values found null to the catch-all category
    remaining_nans_mask = df["val"].isnull() & df["missing_val"].eq(Nans.NOT_MISSING)
    df.loc[remaining_nans_mask, "missing_val"] = Nans.OTHER

    return df

def run_module(params):
    """
    Run the indicator.

    Arguments
    --------
    params:  Dict[str, Any]
        Nested dictionary of parameters.
    """
    start_time = tm.time()
    logger = get_structured_logger(
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))
    base_url = params["indicator"]["base_url"]
    export_start_date = params["indicator"]["export_start_date"]
    export_end_date = params["indicator"]["export_end_date"]
    ## build the base version of the signal at the most detailed geo level you can get.
    all_data = pull_cdcvacc_data(base_url, export_start_date, export_end_date, logger)
    run_stats = []
    ## aggregate & smooth


    if not os.path.exists(params["common"]["export_dir"]):
        os.makedirs(params["common"]["export_dir"])

    for (sensor, smoother, geo) in product(SIGNALS, SMOOTHERS, GEOS):

        logger.info("Running on ",
            sensor=sensor,
            smoother=smoother,
            geo=geo)
        geo_map = geo
        if geo=='state':
            geo_map='state_code'

        df = GeoMapper().replace_geocode(
            all_data[['timestamp','fips', sensor]],
            from_col='fips',
            from_code="fips",
            new_col="geo_id",
            new_code=geo_map,
            date_col="timestamp")
        df["val"] = df[["geo_id", sensor]].groupby("geo_id")[sensor].transform(
            smoother[0].smooth
        )
        df["se"] = None
        df["sample_size"] = None
        df = add_nancodes(df)
        sensor_name = sensor + smoother[1]
        if not (("cumulative" in sensor_name) and ("7dav" in sensor_name)):
            # don't export first 6 days for smoothed signals since they'll be nan.
            start_date = min(df.timestamp) + timedelta(6) if smoother[1] else min(df.timestamp)
            exported_csv_dates = create_export_csv(
                df,
                params["common"]["export_dir"],
                geo,
                sensor_name,
                start_date=start_date)
            if len(exported_csv_dates) > 0:
                run_stats.append((max(exported_csv_dates), len(exported_csv_dates)))
    ## log this indicator run
    elapsed_time_in_seconds = round(tm.time() - start_time, 2)
    min_max_date = run_stats and min(s[0] for s in run_stats)
    csv_export_count = sum(s[-1] for s in run_stats)
    max_lag_in_days = min_max_date and (datetime.now() - min_max_date).days
    formatted_min_max_date = min_max_date and min_max_date.strftime("%Y-%m-%d")
    logger.info("Completed indicator run",
                elapsed_time_in_seconds = elapsed_time_in_seconds,
                csv_export_count = csv_export_count,
                max_lag_in_days = max_lag_in_days,
                oldest_final_export_date = formatted_min_max_date)
