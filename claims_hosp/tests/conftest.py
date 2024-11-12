import logging
import shutil
from pathlib import Path
import re

import copy
import pytest
import mock
import pandas as pd
from unittest.mock import MagicMock

from os import listdir, remove, makedirs
from os.path import join, exists

import delphi_claims_hosp

TEST_DIR = Path(__file__).parent
@pytest.fixture(scope="session")
def params():
    params = {
        "common": {
            "export_dir": f"{TEST_DIR}/retrieve_files",
            "log_filename": f"{TEST_DIR}/test.log",
            "custom_run": False,
        },
        "indicator": {
            "drop_date": None,
            "generate_backfill_files": True,
            "ftp_credentials":
                {"host": "test_host",
                 "user": "test_user",
                 "pass": "test_pass",
                 "port": 2222
            },
            "write_se": False,
            "obfuscated_prefix": "foo_obfuscated",
            "parallel": False,
            "geos": ["nation"],
            "weekday": [True, False],
            "backfill_dir": f"{TEST_DIR}/backfill",
            "start_date": "2020-02-01",
            "end_date": None,
            "drop_date": None,
            "n_backfill_days": 70,
            "n_waiting_days": 3,
            "input_dir": f"{TEST_DIR}/receiving",
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
            "start_issue": "2020-06-12",
            "end_issue": "2020-06-12",
            "patch_dir": "./patch_dir"
        }
    return params_copy

@pytest.fixture(scope="session")
def run_as_module(params):
    with mock.patch('delphi_claims_hosp.patch.get_structured_logger') as mock_get_structured_logger, \
            mock.patch('delphi_claims_hosp.patch.read_params') as mock_read_params, \
            mock.patch('delphi_claims_hosp.download_claims_ftp_files.paramiko.SSHClient') as mock_ssh_client, \
            mock.patch('delphi_claims_hosp.download_claims_ftp_files.path.exists', return_value=False) as mock_exists:
        mock_ssh_client_instance = MagicMock()
        mock_ssh_client.return_value = mock_ssh_client_instance
        mock_sftp = MagicMock()
        mock_ssh_client_instance.open_sftp.return_value = mock_sftp
        mock_sftp.listdir_attr.return_value = [MagicMock(filename="SYNEDI_AGG_INPATIENT_11062020_1451CDT.csv.gz")]
        def mock_get(*args, **kwargs):
            src = Path(f"{TEST_DIR}/test_data/{args[0]}")
            dst = Path(f"{TEST_DIR}/receiving/{args[0]}")
            shutil.copyfile(src, dst)

        mock_sftp.get.side_effect = mock_get
        mock_read_params.return_value = params

        delphi_claims_hosp.run.run_module(params)
