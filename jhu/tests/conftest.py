# -*- coding: utf-8 -*-

from os import listdir, remove
from os.path import join

from boto3 import Session
from moto import mock_s3
import pandas as pd
import pytest

from delphi_jhu.run import run_module

PARAMS =  {
    "common": {
        "export_dir": "./receiving"
    },
    "indicator": {
        "base_url": "test_data/small_{metric}.csv",
        "export_start_date": "2020-03-03",
        "static_file_dir": "../static"
    },
    "archive": {
        "aws_credentials": {
        "aws_access_key_id": "FAKE_TEST_ACCESS_KEY_ID",
        "aws_secret_access_key": "FAKE_TEST_SECRET_ACCESS_KEY"
        },
        "bucket_name": "test-bucket",
        "cache_dir": "./cache"
    }
}

@pytest.fixture(scope="session")
def run_as_module():
    # Clean receiving directory
    for fname in listdir("receiving"):
        if fname != ".gitkeep":
            remove(join("receiving", fname))

    with mock_s3():
        # Create the fake bucket we will be using
        aws_credentials = PARAMS["archive"]["aws_credentials"]
        s3_client = Session(**aws_credentials).client("s3")
        s3_client.create_bucket(Bucket=PARAMS["archive"]["bucket_name"])

        run_module(PARAMS)

@pytest.fixture
def jhu_confirmed_test_data():
    df = pd.read_csv("test_data/jhu_confirmed.csv", dtype={"fips": str})
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df
