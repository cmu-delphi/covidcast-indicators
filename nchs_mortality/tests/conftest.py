# -*- coding: utf-8 -*-

from boto3 import Session
from freezegun import freeze_time
from moto import mock_s3
import pytest

from os import listdir, remove
from os.path import join
from shutil import copy

from delphi_utils import read_params
from delphi_nchs_mortality.run import run_module


@pytest.fixture(scope="function")
def run_as_module(date):
    # Clean directories
    for fname in listdir("receiving"):
        if ".csv" in fname:
            remove(join("receiving", fname))

    for fname in listdir("cache"):
        if ".csv" in fname:
            remove(join("cache", fname))

    for fname in listdir("daily_cache"):
        if ".csv" in fname:
            remove(join("daily_cache", fname))

    # Simulate the cache already being partially populated
    copy("test_data/weekly_202025_state_wip_deaths_covid_incidence_prop.csv", "daily_cache")

    for fname in listdir("daily_receiving"):
        if ".csv" in fname:
            remove(join("daily_receiving", fname))

    with mock_s3():
        with freeze_time(date):
            # Create the fake bucket we will be using
            params = read_params()
            aws_credentials = params["aws_credentials"]
            s3_client = Session(**aws_credentials).client("s3")
            s3_client.create_bucket(Bucket=params["bucket_name"])

            run_module()
