import copy
import json
from pathlib import Path
from unittest.mock import patch

import pytest
from delphi_nssp.constants import DATASET_ID
from delphi_nssp.run import run_module

TEST_DIR = Path(__file__).parent

# test data generated with following url with socrata:
# https://data.cdc.gov/resource/rdmq-nq56.json?$where=week_end >= '2022-10-01T00:00:00.000' AND week_end <= '2022-10-20T00:00:00.000'

with open(f"{TEST_DIR}/test_data/page.json", "r") as f:
    TEST_DATA = json.load(f)

with open(f"{TEST_DIR}/test_data/page_100_hrr.json", "r") as f:
    HRR_TEST_DATA = json.load(f)

with open(f"{TEST_DIR}/test_data/page_no_data.json", "r") as f:
    EMPTY_TEST_DATA = json.load(f)

@pytest.fixture(scope="session")
def params():
    params = {
        "common": {
            "export_dir": f"{TEST_DIR}/receiving",
            "log_filename": f"{TEST_DIR}/test.log",
            "backup_dir": f"{TEST_DIR}/test_raw_data_backups",
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
            "source_dir": "test_source_dir",
            "source_host": "host",
            "user": "test_user",
            "start_issue": "2023-01-01",
            "end_issue": "2023-01-03",
        }

    return params_copy

@pytest.fixture(scope="function")
def run_as_module(params):
    """
    Fixture to use TEST_DATA when testing run_module.

    This fixture patches Socrara to return the predefined test data
    """

    with patch('sodapy.Socrata.get') as mock_get:
        def side_effect(*args, **kwargs):
            if kwargs['offset'] == 0:
                if DATASET_ID in args[0]:
                    return TEST_DATA
            else:
                return []
        mock_get.side_effect = side_effect
        run_module(params)



@pytest.fixture(scope="function")
def run_as_module_hrr(params):
    """
    Fixture to use HRR_TEST_DATA when testing run_module.

    This fixture patches socrara to return the predefined test data for HRR region.
    """

    with patch('sodapy.Socrata.get') as mock_get, \
         patch('delphi_nssp.run.GEOS', ["hrr"]):
        def side_effect(*args, **kwargs):
            if kwargs['offset'] == 0:
                if DATASET_ID in args[0]:
                    return HRR_TEST_DATA
            else:
                return []
        mock_get.side_effect = side_effect
        run_module(params)
