# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_google_symptoms`.
"""
import time
from datetime import date, datetime
from itertools import product

import numpy as np
from delphi_utils import create_export_csv, get_structured_logger

from .constants import COMBINED_METRIC, FULL_BKFILL_START_DATE, GEO_RESOLUTIONS, SMOOTHERS, SMOOTHERS_MAP
from .date_utils import generate_num_export_days
from .geo import geo_map
from .pull import pull_gs_data


def run_module(params, logger=None):
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
    export_dir = params["common"]["export_dir"]

    if logger is None:
        logger = get_structured_logger(
            __name__,
            filename=params["common"].get("log_filename"),
            log_exceptions=params["common"].get("log_exceptions", True),
        )

    export_start_date = datetime.strptime(
        params["indicator"].get("export_start_date", datetime.strftime(FULL_BKFILL_START_DATE, "%Y-%m-%d")), "%Y-%m-%d"
    )
    # If end_date not specified, use current date.
    export_end_date = datetime.strptime(
        params["indicator"].get("export_end_date", datetime.strftime(date.today(), "%Y-%m-%d")), "%Y-%m-%d"
    )

    num_export_days = generate_num_export_days(params, logger)
    # safety check for patch parameters exists in file, but not running custom runs/patches
    custom_run_flag = (
        False if not params["indicator"].get("custom_run", False) else params["indicator"].get("custom_run", False)
    )

    # Pull GS data
    dfs = pull_gs_data(
        params["indicator"]["bigquery_credentials"],
        export_start_date,
        export_end_date,
        num_export_days,
        custom_run_flag,
    )
    for geo_res in GEO_RESOLUTIONS:
        if geo_res == "state":
            df_pull = dfs["state"]
        elif geo_res in ["hhs", "nation"]:
            df_pull = geo_map(dfs["state"], geo_res)
        else:
            df_pull = geo_map(dfs["county"], geo_res)

        if len(df_pull) == 0:
            continue
        for metric, smoother in product(COMBINED_METRIC, SMOOTHERS):
            logger.info("Generating signal and exporting to CSV", geo_type=geo_res, signal=f"{geo_res}_{metric}_{sensor_name}")
            df = df_pull
            df["val"] = df[metric].astype(float)
            df["val"] = df[["geo_id", "val"]].groupby(
                "geo_id")["val"].transform(
                SMOOTHERS_MAP[smoother][0].smooth)
            df["se"] = np.nan
            df["sample_size"] = np.nan
            # Drop early entries where data insufficient for smoothing
            df = df.loc[~df["val"].isnull(), :]
            df = df.reset_index()
            sensor_name = "_".join([smoother, "search"])
            if len(df) == 0:
                logger.info("No data for signal", geo_type=geo_res, signal=f"{geo_res}_{metric}_{sensor_name}")
                continue
            exported_csv_dates = create_export_csv(
                df,
                export_dir=export_dir,
                start_date=SMOOTHERS_MAP[smoother][1](export_start_date),
                metric=metric.lower(),
                geo_res=geo_res,
                sensor=sensor_name)
            if not exported_csv_dates.empty:
                logger.info("Exported CSV",
                            csv_export_count=exported_csv_dates.size,
                            min_csv_export_date=min(exported_csv_dates).strftime("%Y-%m-%d"),
                            max_csv_export_date=max(exported_csv_dates).strftime("%Y-%m-%d"))
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
