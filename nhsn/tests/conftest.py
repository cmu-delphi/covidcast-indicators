import copy
import json
from unittest.mock import patch

import pytest
from pathlib import Path

from freezegun import freeze_time

from delphi_nhsn.run import run_module

TEST_DIR = Path(__file__).parent

# test data generated with following url with socrata:
# https://data.cdc.gov/resource/ua7e-t2fy.json?$where=weekendingdate%20between%20%272021-08-19T00:00:00.000%27%20and%20%272021-10-19T00:00:00.000%27%20and%20jurisdiction%20in(%27CO%27,%27USA%27)
# preliminary source
# https://data.cdc.gov/resource/mpgq-jmmr.json?$where=weekendingdate%20between%20%272021-08-19T00:00:00.000%27%20and%20%272021-10-19T00:00:00.000%27%20and%20jurisdiction%20in(%27CO%27,%27USA%27)
# queries the nhsn data with timestamp (2021-08-19, 2021-10-19) with CO and USA data


with open("test_data/page.json", "r") as f:
    TEST_DATA = json.load(f)

with open("test_data/prelim_page.json", "r") as f:
    PRELIM_TEST_DATA = json.load(f)

@pytest.fixture(scope="session")
def params():
    params = {
        "common": {
            "export_dir": f"{TEST_DIR}/receiving",
            "log_filename": f"{TEST_DIR}/test.log",
            "backup_dir": f"{TEST_DIR}/backups",
            "custom_run": False
        },
        "indicator": {
            "wip_signal": True,
            "export_start_date": "2020-08-01",
            "static_file_dir": "./static",
            "socrata_token": "test_token"
        },
        "validation": {
            "common": {
                "span_length": 14,
                "min_expected_lag": {"all": "3"},
                "max_expected_lag": {"all": "4"},
            }
        }
    }
    return copy.deepcopy(params)

@pytest.fixture
def params_w_patch(params):
    params_copy = copy.deepcopy(params)
    params_copy["patch"] = {
            "start_issue": "2024-06-27",
            "end_issue": "2024-06-29",
            "patch_dir": "./patch_dir"
        }
    return params_copy

@pytest.fixture(scope="function")
def run_as_module(params, run_type):
    issue_date = ""
    if run_type == "prelim_run":
        issue_date = "2021-10-21"
    elif run_type == "regular_run":
        issue_date = "2021-10-23"

    with freeze_time(issue_date):
        with patch('sodapy.Socrata.get') as mock_get:
            def side_effect(*args, **kwargs):
                if kwargs['offset'] == 0:
                    if "ua7e-t2fy" in args[0]:
                        return TEST_DATA
                    if "mpgq-jmmr" in args[0]:
                        return PRELIM_TEST_DATA
                else:
                    return []
            mock_get.side_effect = side_effect
            run_module(params)

