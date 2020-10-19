"""Functions to call when running the function.
This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.
"""
import glob
import multiprocessing as mp
import subprocess

from delphi_utils import read_params

from .constants import SIGNALS, GEO_RESOLUTIONS
from .process import process, add_prefix, files_in_past_week


def run_module():
    """Creates the Safegraph indicator."""
    params = read_params()
    export_dir = params["export_dir"]
    raw_data_dir = params["raw_data_dir"]
    n_core = int(params["n_core"])
    aws_access_key_id = params["aws_access_key_id"]
    aws_secret_access_key = params["aws_secret_access_key"]
    aws_default_region = params["aws_default_region"]
    aws_endpoint = params["aws_endpoint"]
    wip_signal = params["wip_signal"]

    def process_file(current_filename):
        """Wrapper around `process()` that only takes a single argument.

        A single argument function is necessary to use `pool.map()` below.
        Because each call to `process()` has two arguments that are dependent
        on the input file name (`current_filename` and `previous_filenames`),
        we choose to use this wrapper rather than something like
        `functools.partial()`.
        """
        return process(current_filename,
                       files_in_past_week(current_filename),
                       signal_names=SIGNALS,
                       wip_signal=wip_signal,
                       geo_resolutions=GEO_RESOLUTIONS,
                       export_dir=export_dir,
                       )

    # Update raw data
    # Why call subprocess rather than using a native Python client, e.g. boto3?
    # Because boto3 does not have a simple rsync-like call that can perform
    # the following behavior elegantly.
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

    with mp.Pool(n_core) as pool:
        pool.map(process_file, files)
