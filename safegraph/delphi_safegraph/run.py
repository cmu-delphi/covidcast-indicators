# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.
"""
import glob
import multiprocessing as mp
import subprocess
from datetime import datetime
from functools import partial

import numpy as np
import pandas as pd
from delphi_utils import read_params

from .process import process

SIGNALS = [
    # signal_name                wip
    ('median_home_dwell_time',   False),
    ('prop_completely_home',     False),
    ('prop_full_time_work',      False),
    ('prop_part_time_work',      False),
]
GEO_RESOLUTIONS = [
    'county',
    'state',
]


def run_module():

    params = read_params()
    export_dir = params["export_dir"]
    raw_data_dir = params["raw_data_dir"]
    n_core = int(params["n_core"])
    aws_access_key_id = params["aws_access_key_id"]
    aws_secret_access_key = params["aws_secret_access_key"]
    aws_default_region = params["aws_default_region"]
    aws_endpoint = params["aws_endpoint"]

    process_file = partial(process,
            signals=SIGNALS,
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
        )

    files = glob.glob(f'{raw_data_dir}/social-distancing/**/*.csv.gz',
            recursive=True)

    with mp.Pool(n_core) as pool:
        pool.map(process_file, files)

