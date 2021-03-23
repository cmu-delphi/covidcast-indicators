# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.
"""
import glob
import multiprocessing as mp
import subprocess
import time
from functools import partial
from os.path import join

import pandas as pd
from delphi_utils import get_structured_logger

from .process import process


METRICS = [
        # signal_name, naics_code, wip
        ('bars_visit', 722410, False),
        ('restaurants_visit', 722511, False),
]
VERSIONS = [
        # relaese version, access dir
        ("202004", "weekly-patterns/v2", "main-file/*.csv.gz"),
        ("202006", "weekly-patterns-delivery/weekly", "patterns/*/*/*")
]
SENSORS = [
        "num",
        "prop"
]
GEO_RESOLUTIONS = [
        "county",
        "hrr",
        "msa",
        "state",
        "hhs",
        "nation"
]


def run_module(params):
    """Run module for Safegraph patterns data.

    The `params` argument is expected to have the following structure:
    - "common":
        - "export_dir": str, directory to write output
        - "log_exceptions" (optional): bool, whether to log exceptions to file
        - "log_filename" (optional): str, name of file to write logs
    - "indicator":
        - "aws_access_key_id": str, ID of access key for AWS S3
        - "aws_secret_access_key": str, access key for AWS S3
        - "aws_default_region": str, name of AWS S3 region
        - "aws_endpoint": str, name of AWS S3 endpoint
        - "n_core": int, number of cores to use for multithreaded processing
        - "raw_data_dir": directory from which to read downloaded data from AWS,
        - "static_file_dir": str, directory containing brand and population csv files
        - "sync": bool, whether to sync S3 data before running indicator
    """
    start_time = time.time()
    export_dir = params["common"]["export_dir"]
    raw_data_dir = params["indicator"]["raw_data_dir"]
    n_core = params["indicator"]["n_core"]
    aws_endpoint = params["indicator"]["aws_endpoint"]
    static_file_dir = params["indicator"]["static_file_dir"]
    logger = get_structured_logger(
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))
    env_vars = {
            'AWS_ACCESS_KEY_ID': params["indicator"]["aws_access_key_id"],
            'AWS_SECRET_ACCESS_KEY': params["indicator"]["aws_secret_access_key"],
            'AWS_DEFAULT_REGION': params["indicator"]["aws_default_region"],
    }

    for ver in VERSIONS:
        # Update raw data
        # Why call subprocess rather than using a native Python client, e.g. boto3?
        # Because boto3 does not have a simple rsync-like call that can perform
        # the following behavior elegantly.
        if params["indicator"]["sync"]:
            subprocess.run(
                    f'aws s3 sync s3://sg-c19-response/{ver[1]}/ '
                    f'{raw_data_dir}/{ver[1]}/ --endpoint {aws_endpoint}',
                    env=env_vars,
                    shell=True,
                    check=True
            )

        brand_df = pd.read_csv(
                join(static_file_dir, f"brand_info/brand_info_{ver[0]}.csv")
        )

        files = glob.glob(f'{raw_data_dir}/{ver[1]}/{ver[2]}',
                recursive=True)

        process_file = partial(process, brand_df=brand_df,
                               metrics=METRICS,
                               sensors=SENSORS,
                               geo_resolutions=GEO_RESOLUTIONS,
                               export_dir=export_dir,
                               )

        with mp.Pool(n_core) as pool:
            pool.map(process_file, files)

    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    logger.info("Completed indicator run",
        elapsed_time_in_seconds = elapsed_time_in_seconds)
