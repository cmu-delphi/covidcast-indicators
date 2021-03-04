# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_google_symptoms`.
"""
import time
from datetime import datetime
from itertools import product

import numpy as np
from delphi_utils import (
    create_export_csv,
    geomap,
    get_structured_logger
)

from .constants import (METRICS, COMBINED_METRIC,
                        GEO_RESOLUTIONS, SMOOTHERS, SMOOTHERS_MAP)
from .geo import geo_map
from .pull import pull_gs_data


def run_module(params):
    """
    Run Google Symptoms module.

    Parameters
    ----------
    params
        Dictionary containing indicator configuration. Expected to have the following structure:
    - "common":
        - "export_dir": str, directory to write output
        - "log_exceptions" (optional): bool, whether to log exceptions to file
        - "log_filename" (optional): str, name of file to write logs
    - "indicator":
        - "export_start_date": str, YYYY-MM-DD format, date from which to export data
        - "num_export_days": int, number of days before end date (today) to export
        - "path_to_bigquery_credentials": str, path to BigQuery API key and service account
            JSON file
    """
    start_time = time.time()
    csv_export_count = 0
    oldest_final_export_date = None

    export_start_date = datetime.strptime(
        params["indicator"]["export_start_date"], "%Y-%m-%d")
    export_dir = params["common"]["export_dir"]
    num_export_days = params["indicator"].get("num_export_days", "all")

    logger = get_structured_logger(
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))

    # Pull GS data
    dfs = pull_gs_data(params["indicator"]["bigquery_credentials"],
                       export_start_date,
                       num_export_days)
    gmpr = geomap.GeoMapper()

    for geo_res in GEO_RESOLUTIONS:
        if geo_res == "state":
            df_pull = dfs["state"]
        elif geo_res in ["hhs", "nation"]:
            df_pull = gmpr.replace_geocode(dfs["county"], "fips", geo_res, from_col="geo_id",
                                           date_col="timestamp")
            df_pull.rename(columns={geo_res: "geo_id"}, inplace=True)
        else:
            df_pull = geo_map(dfs["county"], geo_res)

        if len(df_pull) == 0:
            continue
        for metric, smoother in product(
                METRICS+[COMBINED_METRIC], SMOOTHERS):
            print(geo_res, metric, smoother)
            df = df_pull.set_index(["timestamp", "geo_id"])
            df["val"] = df[metric].groupby(level=1
                                           ).transform(SMOOTHERS_MAP[smoother][0])
            df["se"] = np.nan
            df["sample_size"] = np.nan
            # Drop early entries where data insufficient for smoothing
            df = df.loc[~df["val"].isnull(), :]
            df = df.reset_index()
            sensor_name = "_".join([smoother, "search"])

            if len(df) == 0:
                continue
            exported_csv_dates = create_export_csv(
                df,
                export_dir=export_dir,
                start_date=SMOOTHERS_MAP[smoother][1](export_start_date),
                metric=metric.lower(),
                geo_res=geo_res,
                sensor=sensor_name)

            if not exported_csv_dates.empty:
                csv_export_count += exported_csv_dates.size
                if not oldest_final_export_date:
                    oldest_final_export_date = max(exported_csv_dates)
                oldest_final_export_date = min(
                    oldest_final_export_date, max(exported_csv_dates))

    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    max_lag_in_days = None
    formatted_oldest_final_export_date = None
    if oldest_final_export_date:
        max_lag_in_days = (datetime.now() - oldest_final_export_date).days
        formatted_oldest_final_export_date = oldest_final_export_date.strftime(
            "%Y-%m-%d")
    logger.info("Completed indicator run",
                elapsed_time_in_seconds=elapsed_time_in_seconds,
                csv_export_count=csv_export_count,
                max_lag_in_days=max_lag_in_days,
                oldest_final_export_date=formatted_oldest_final_export_date)
