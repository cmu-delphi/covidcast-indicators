"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.
"""
import functools
import multiprocessing as mp
import subprocess
import time
from datetime import timedelta

from delphi_utils import get_structured_logger

from .constants import SIGNALS, GEO_RESOLUTIONS
from .process import process, get_daily_source_files


def run_module(params):
    """Create the Safegraph indicator.

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
        - "raw_data_dir": str, directory from which to read downloaded data from AWS,
        - "static_file_dir": str, directory containing brand and population csv files
        - "sync": bool, whether to sync S3 data before running indicator
        - "wip_signal": list of str or bool, list of work-in-progress signals to be passed to
                        `delphi_utils.add_prefix()`
    """
    start_time = time.time()
    logger = get_structured_logger(
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))

    # Place to write output files.
    export_dir = params["common"]["export_dir"]
    # Location of input files.
    raw_data_dir = params["indicator"]["raw_data_dir"]

    # Number of cores to use in multiprocessing.
    n_core = params["indicator"]["n_core"]

    # AWS credentials
    aws_access_key_id = params["indicator"]["aws_access_key_id"]
    aws_secret_access_key = params["indicator"]["aws_secret_access_key"]
    aws_default_region = params["indicator"]["aws_default_region"]
    aws_endpoint = params["indicator"]["aws_endpoint"]
    # Whether to sync `raw_data_dir` with an AWS backend.
    # Must be a bool in the JSON file (rather than the string "True" or "False")
    sync = params["indicator"]["sync"]

    # List of work-in-progress signal names.
    wip_signal = params["indicator"]["wip_signal"]

    # Convert `process()` to a single-argument function for use in `pool.map`.
    single_arg_process = functools.partial(
        process,
        signal_names=SIGNALS,
        wip_signal=wip_signal,
        geo_resolutions=GEO_RESOLUTIONS,
        export_dir=export_dir,
    )

    # Update raw data
    # Why call subprocess rather than using a native Python client, e.g. boto3?
    # Because boto3 does not have a simple rsync-like call that can perform
    # the following behavior elegantly.
    if sync:
        subprocess.run(
            f'aws s3 sync s3://sg-c19-response/social-distancing/v2/ '
            f'{raw_data_dir}/social-distancing/ --endpoint {aws_endpoint}',
            env={
                'AWS_ACCESS_KEY_ID': aws_access_key_id,
                'AWS_SECRET_ACCESS_KEY': aws_secret_access_key,
                'AWS_DEFAULT_REGION': aws_default_region,
            },
            shell=True,
            check=True,
        )

    files = get_daily_source_files(f'{raw_data_dir}/social-distancing/**/*.csv.gz')

    files_with_previous_weeks = []
    for day in files:
        previous_week = [files[day]]
        for i in range(1, 7):
            if day - timedelta(i) in files:
                previous_week.append(files[day - timedelta(i)])
        files_with_previous_weeks.append(previous_week)

    with mp.Pool(n_core) as pool:
        pool.map(single_arg_process, files_with_previous_weeks)

    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    logger.info("Completed indicator run",
        elapsed_time_in_seconds = elapsed_time_in_seconds)
