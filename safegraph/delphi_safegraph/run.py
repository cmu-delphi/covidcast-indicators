"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.
"""
import glob
import functools
import multiprocessing as mp
import subprocess

from delphi_utils import read_params

from .constants import SIGNALS, GEO_RESOLUTIONS
from .process import process, files_in_past_week


def run_module():
    """Create the Safegraph indicator."""
    params = read_params()

    # Place to write output files.
    export_dir = params["export_dir"]
    # Location of input files.
    raw_data_dir = params["raw_data_dir"]

    # Number of cores to use in multiprocessing.
    n_core = int(params["n_core"])

    # AWS credentials
    aws_access_key_id = params["aws_access_key_id"]
    aws_secret_access_key = params["aws_secret_access_key"]
    aws_default_region = params["aws_default_region"]
    aws_endpoint = params["aws_endpoint"]
    # Whether to sync `raw_data_dir` with an AWS backend.
    # Must be a bool in the JSON file (rather than the string "True" or "False")
    sync = params["sync"]

    # List of work-in-progress signal names.
    wip_signal = params["wip_signal"]

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

    files = glob.glob(f'{raw_data_dir}/social-distancing/**/*.csv.gz',
                      recursive=True)

    files_with_previous_weeks = []
    for fname in files:
        previous_week = [fname]
        previous_week.extend(files_in_past_week(fname))
        files_with_previous_weeks.append(previous_week)

    with mp.Pool(n_core) as pool:
        pool.map(single_arg_process, files_with_previous_weeks)
