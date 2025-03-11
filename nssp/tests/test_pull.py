import glob
import json
from unittest.mock import patch, MagicMock
import os
import shutil
import time
from datetime import datetime
import pdb

import unittest.mock as mock
import pandas as pd

from delphi_nssp.pull import (
    get_source_data,
    pull_nssp_data,
)

from delphi_nssp.constants import (
    SIGNALS,
)

from delphi_utils import get_structured_logger
from conftest import TEST_DATA

class TestPullNSSPData:
    def test_get_source_data(self, params_w_patch):
        logger = get_structured_logger()

        # Create a mock SSH client
        mock_ssh = MagicMock()
        mock_sftp = MagicMock()
        mock_sftp.stat = MagicMock()
        mock_ssh.open_sftp.return_value = mock_sftp

        source_path = params_w_patch["common"]["backup_dir"]
        dest_path = params_w_patch["patch"]["source_dir"]

        dates = pd.date_range(params_w_patch["patch"]["start_issue"], params_w_patch["patch"]["end_issue"])
        files = [f"{date.strftime('%Y%m%d')}.csv.gz" for date in dates]

        with patch("paramiko.SSHClient", return_value=mock_ssh):
            get_source_data(params_w_patch, logger)

        mock_sftp.chdir.assert_called_once_with(source_path)

        for file in files:
            mock_sftp.get.has_calls(file, f"{dest_path}/{file}", mock.ANY)
    @patch("delphi_nssp.pull.Socrata")
    def test_normal_pull_nssp_data(self, mock_socrata, params, caplog):
        today = pd.Timestamp.today().strftime("%Y%m%d")
        backup_dir = params["common"]["backup_dir"]
        custom_run = params["common"]["custom_run"]
        test_token = params["indicator"]["socrata_token"]

        # Mock Socrata client and its get method
        mock_client = MagicMock()
        mock_client.get.side_effect = [TEST_DATA, []]  # Return test data on first call, empty list on second call
        mock_socrata.return_value = mock_client

        logger = get_structured_logger()
        # Call function with test token
        result = pull_nssp_data(test_token, backup_dir, custom_run, logger=logger)

        # Check logger used:
        assert "Backup file created" in caplog.text

        # Check that backup file was created
        backup_files = glob.glob(f"{backup_dir}/{today}.*")
        assert len(backup_files) == 2, "Backup file was not created"

        expected_data = pd.DataFrame(TEST_DATA)
        for backup_file in backup_files:
            if backup_file.endswith(".csv.gz"):
                dtypes = expected_data.dtypes.to_dict()
                actual_data = pd.read_csv(backup_file, dtype=dtypes)
            else:
                actual_data = pd.read_parquet(backup_file)
            pd.testing.assert_frame_equal(expected_data, actual_data)

        # Check that Socrata client was initialized with correct arguments
        mock_socrata.assert_called_once_with("data.cdc.gov", test_token)

        # Check that get method was called with correct arguments
        mock_client.get.assert_any_call("rdmq-nq56", limit=50000, offset=0)

        # Check result
        assert result["timestamp"].notnull().all(), "timestamp has rogue NaN"
        assert result["geography"].notnull().all(), "geography has rogue NaN"
        assert result["county"].notnull().all(), "county has rogue NaN"
        assert result["fips"].notnull().all(), "fips has rogue NaN"
        assert result["fips"].apply(lambda x: isinstance(x, str) and len(x) != 4).all(), "fips formatting should always be 5 digits; include leading zeros if aplicable"

        # Check for each signal in SIGNALS
        for signal in SIGNALS:
            assert result[signal].notnull().all(), f"{signal} has rogue NaN"

        for file in backup_files:
            os.remove(file)
