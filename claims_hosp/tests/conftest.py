import copy
from pathlib import Path

import pytest

TEST_DIR = Path(__file__).parent


@pytest.fixture
def params():
    return copy.deepcopy({
        "common": {
            "export_dir": f"{TEST_DIR}/receiving",
            "log_filename": f"{TEST_DIR}/test.log",
            "custom_run": False,
        },
        "indicator": {
            "input_dir": f"{TEST_DIR}/test_data",
            "start_date": None,
            "end_date": None,
            "drop_date": None,
            "n_backfill_days": 70,
            "n_waiting_days": 3,
            "generate_backfill_files": False,
            "backfill_dir": f"{TEST_DIR}/backfill",
            "backfill_merge_day": 0,
            "write_se": False,
            "obfuscated_prefix": "foo_obfuscated",
            "parallel": False,
            "geos": ["nation"],
            "weekday": [False],
            "ftp_credentials": {
                "host": "test_host",
                "user": "test_user",
                "pass": "test_pass",
                "port": 2222,
            },
        },
    })


@pytest.fixture
def params_w_patch(params):
    params_copy = copy.deepcopy(params)
    params_copy["common"]["custom_run"] = True
    params_copy["patch"] = {
        "start_issue": "2020-06-11",
        "end_issue": "2020-06-11",
        "patch_dir": f"{TEST_DIR}/patch_dir",
    }
    return params_copy
