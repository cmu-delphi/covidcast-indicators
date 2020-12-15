# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.
"""
import glob
import multiprocessing as mp
import subprocess
from functools import partial
from os.path import join

import pandas as pd
from delphi_utils import read_params

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


def run_module():
    """Run module for Safegraph patterns data."""
    params = read_params()
    export_dir = params["export_dir"]
    raw_data_dir = params["raw_data_dir"]
    n_core = int(params["n_core"])
    aws_endpoint = params["aws_endpoint"]
    static_file_dir = params["static_file_dir"]

    env_vars = {
            'AWS_ACCESS_KEY_ID': params["aws_access_key_id"],
            'AWS_SECRET_ACCESS_KEY': params["aws_secret_access_key"],
            'AWS_DEFAULT_REGION': params["aws_default_region"],
    }

    for ver in VERSIONS:
        # Update raw data
        # Why call subprocess rather than using a native Python client, e.g. boto3?
        # Because boto3 does not have a simple rsync-like call that can perform
        # the following behavior elegantly.
        if params["sync"]:
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
