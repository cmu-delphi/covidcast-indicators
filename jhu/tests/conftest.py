# -*- coding: utf-8 -*-

from boto3 import Session
from moto import mock_s3
import pytest

from os import listdir, remove
from os.path import join
import pandas as pd

from delphi_utils import read_params
from delphi_jhu.run import run_module


@pytest.fixture(scope="session")
def run_as_module():
    # Clean receiving directory
    for fname in listdir("receiving"):
        if fname != ".gitkeep":
            remove(join("receiving", fname))

    with mock_s3():
        # Create the fake bucket we will be using
        params = read_params()
        aws_credentials = params["aws_credentials"]
        s3_client = Session(**aws_credentials).client("s3")
        s3_client.create_bucket(Bucket=params["bucket_name"])

        run_module()

@pytest.fixture
def jhu_confirmed_test_data():
    df = pd.read_csv("test_data/jhu_confirmed.csv", dtype={"fips": str})
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df
