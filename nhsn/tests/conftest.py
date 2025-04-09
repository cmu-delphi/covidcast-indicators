import copy
import json
import time
from unittest.mock import patch

import pytest
from pathlib import Path

from delphi_nhsn.run import run_module

TEST_DIR = Path(__file__).parent

# test data generated with following url with socrata:
# https://data.cdc.gov/resource/ua7e-t2fy.json?$where=weekendingdate%20between%20%272021-08-19T00:00:00.000%27%20and%20%272021-10-19T00:00:00.000%27%20and%20jurisdiction%20in(%27CO%27,%27USA%27)
# preliminary source
# https://data.cdc.gov/resource/mpgq-jmmr.json?$where=weekendingdate%20between%20%272021-08-19T00:00:00.000%27%20and%20%272021-10-19T00:00:00.000%27%20and%20jurisdiction%20in(%27CO%27,%27USA%27)
# queries the nhsn data with timestamp (2021-08-19, 2021-10-19) with CO and USA data


with open(f"{TEST_DIR}/test_data/page.json", "r") as f:
    TEST_DATA = json.load(f)

with open(f"{TEST_DIR}/test_data/prelim_page.json", "r") as f:
    PRELIM_TEST_DATA = json.load(f)

# filtered metadata (just includes nhsn meta)
with open(f"{TEST_DIR}/test_data/covidcast_meta.json", "r") as f:
    COVID_META_DATA = json.load(f)


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
    params_copy["common"]["custom_run"] = True
    params_copy["patch"] = {
            "patch_dir": f"{TEST_DIR}/patch_dir",
            "issue_date": "2024-12-12",
        }

    return params_copy

@pytest.fixture(scope="function")
def run_as_module(params):
    with patch('sodapy.Socrata.get') as mock_get, \
         patch('sodapy.Socrata.get_metadata') as mock_get_metadata:
        def side_effect(*args, **kwargs):
            if kwargs['offset'] == 0:
                if "ua7e-t2fy" in args[0]:
                    return TEST_DATA
                if "mpgq-jmmr" in args[0]:
                    return PRELIM_TEST_DATA
            else:
                return []
        mock_get.side_effect = side_effect
        mock_get_metadata.return_value = {"rowsUpdatedAt": time.time()}
        run_module(params)

