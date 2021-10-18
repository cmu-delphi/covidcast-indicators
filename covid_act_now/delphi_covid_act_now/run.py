# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_covid_act_now`.
"""
from datetime import datetime
import time

import numpy as np

from delphi_utils import (
    create_export_csv,
    get_structured_logger
)

from .constants import GEO_RESOLUTIONS, SIGNALS
from .geo import geo_map
from .pull import load_data, extract_testing_metrics

def run_module(params):
    """
    Run the CAN testing metrics indicator.

    Parameters
    ----------
    params
        Dictionary containing indicator configuration. Expected to have the following structure:
        - "common":
            - "export_dir": str, directory to write output
        - "indicator":
            - "parquet_url": str, URL of source file in parquet format
        - "archive" (optional): if provided, output will be archived with S3
            - "cache_dir": str, directory of locally cached data
            - "bucket_name: str, name of S3 bucket to read/write
            - "aws_credentials": Dict[str, str], AWS login credentials (see S3 documentation)
    """
    start_time = time.time()
    logger = get_structured_logger(
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))

    # Configuration
    export_dir = params["common"]["export_dir"]
    parquet_url = params["indicator"]["parquet_url"]

    # Load CAN county-level testing data
    logger.info("Pulling CAN data")
    df_pq = load_data(parquet_url)
    df_county_testing = extract_testing_metrics(df_pq)

    num_exported_files = 0
    min_dates_exported = []
    max_dates_exported = []
    # Perform geo aggregations and export to receiving
    for geo_res in GEO_RESOLUTIONS:
        logger.info("Generating signal and exporting to CSV",
                    geo_res = geo_res)
        df = geo_map(df_county_testing, geo_res)

        # Export 'pcr_specimen_positivity_rate'
        exported_csv_dates = create_export_csv(
            df,
            export_dir=export_dir,
            geo_res=geo_res,
            sensor=SIGNALS[0])

        # Export 'pcr_specimen_total_tests'
        df["val"] = df["sample_size"]
        df["sample_size"] = np.nan
        df["se"] = np.nan
        exported_csv_dates = create_export_csv(
            df,
            export_dir=export_dir,
            geo_res=geo_res,
            sensor=SIGNALS[1])

        earliest, latest = min(exported_csv_dates), max(exported_csv_dates)
        min_dates_exported.append(earliest)
        max_dates_exported.append(latest)
        # x2 to count both positivity and tests signals
        num_exported_files += exported_csv_dates.size * 2
        logger.info("Exported for dates between", earliest=earliest, latest=latest)

    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    max_lag_in_days = (datetime.now() - min(max_dates_exported)).days
    logger.info("Completed indicator run",
                elapsed_time_in_seconds=elapsed_time_in_seconds,
                csv_export_count=num_exported_files,
                max_lag_in_days=max_lag_in_days,
                earliest_export_date=min(min_dates_exported).strftime("%Y-%m-%d"),
                latest_export_date=max(max_dates_exported).strftime("%Y-%m-%d"))
