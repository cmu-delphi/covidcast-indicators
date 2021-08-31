# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.  `run_module`'s lone argument should be a
nested dictionary of parameters loaded from the params.json file.  We expect the `params` to have
the following structure:
    - "common":
        - "export_dir": str, directory to which the results are exported
        - "log_filename": (optional) str, path to log file
    - "indicator": (optional)
        - "wip_signal": (optional) Any[str, bool], list of signals that are works in progress, or
            True if all signals in the registry are works in progress, or False if only
            unpublished signals are.  See `delphi_utils.add_prefix()`
        - Any other indicator-specific settings
"""
from datetime import timedelta
import pandas as pd
import time

from delphi_utils.export import create_export_csv
from delphi_utils.geomap import GeoMapper
from delphi_utils import get_structured_logger

from .constants import GEOS, SIGNALS, SMOOTHERS

def run_module(params):
    """
    Runs the indicator

    Arguments
    --------
    params:  Dict[str, Any]
        Nested dictionary of parameters.
    """
    start_time = time.time()
    logger = get_structured_logger(
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))
    mapper = GeoMapper()
    run_stats = []
    ## build the base version of the signal at the most detailed geo level you can get.
    ## compute stuff here or farm out to another function or file
    all_data = pd.DataFrame(columns=["timestamp", "val", "zip", "sample_size", "se"])
    ## aggregate & smooth
    ## TODO: add num/prop variations if needed
    for sensor, smoother, geo in product(SIGNALS, SMOOTHERS, GEOS):
        df = mapper.replace_geocode(
            all_data, "zip", geo,
            new_col="geo_id",
            date_col="timestamp")
        ## TODO: recompute sample_size, se here if not NA
        df["val"] = df[["geo_id", "val"]].groupby("geo_id")["val"].transform(
            smoother[0].smooth
        )
        sensor_name = sensor + smoother[1] ## TODO: +num/prop variation if used
        # don't export first 6 days for smoothed signals since they'll be nan.
        start_date = min(df.timestamp) + timedelta(6) if smoother[1] else min(df.timestamp)
        dates = create_export_csv(
            df,
            params["common"]["export_dir"],
            geo,
            sensor_name,
            start_date=start_date)
        if len(dates) > 0:
            run_stats.append((max(dates), len(dates)))
    ## log this indicator run
    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    min_max_date = run_stats and min(s[0] for s in run_stats)
    csv_export_count = sum(s[-1] for s in run_stats)
    max_lag_in_days = min_max_date and (datetime.now() - min_max_date).days
    formatted_min_max_date = min_max_date and min_max_date.strftime("%Y-%m-%d")
    logger.info("Completed indicator run",
                elapsed_time_in_seconds = elapsed_time_in_seconds,
                csv_export_count = csv_export_count,
                max_lag_in_days = max_lag_in_days,
                oldest_final_export_date = formatted_min_max_date)
