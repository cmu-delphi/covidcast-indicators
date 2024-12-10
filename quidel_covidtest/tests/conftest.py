# -*- coding: utf-8 -*-
from datetime import datetime
from os.path import join
import os
from pathlib import Path

import mock
import pandas as pd
import pytest
from mock.mock import patch

import delphi_quidel_covidtest.run

TEST_DIR = Path(__file__).parent
SOURCE_DIR = Path(__file__).parent.parent
@pytest.fixture(scope='session')
def params():
    PARAMS = {
        "common": {
            "export_dir": f"{TEST_DIR}/receiving"
        },
        "indicator": {
            "static_file_dir": f"{SOURCE_DIR}/static",
            "input_cache_dir": f"{TEST_DIR}/cache",
            "backfill_dir": f"{TEST_DIR}/backfill",
            "backfill_merge_day": 0,
            "export_start_date": "2020-06-30",
            "export_end_date": "",
            "pull_start_date": "2020-07-09",
            "pull_end_date":"",
            "export_day_range":40,
            "aws_credentials": {
                "aws_access_key_id": "",
                "aws_secret_access_key": ""
            },
            "bucket_name": "",
            "wip_signal": "",
            "test_mode": True
        }
    }
    return PARAMS

@pytest.fixture(scope="session", autouse=True)
def mock_get_from_s3():
    with patch("delphi_quidel_covidtest.pull.get_from_s3") as m:
        test_data_dir = "./test_data/test_data.csv"
        time_flag = datetime(2020, 8, 17)
        df = pd.read_csv(
            test_data_dir,
            parse_dates=["StorageDate", "TestDate"]
        )
        m.return_value = df, time_flag
        yield m

@pytest.fixture(scope="session")
def run_as_module(params, mock_get_from_s3):
    delphi_quidel_covidtest.run.run_module(params)
