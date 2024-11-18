import glob
import json
from unittest.mock import patch, MagicMock
import os
import pytest

import pandas as pd

from delphi_nhsn.pull import (
    pull_nhsn_data,
)
from delphi_nhsn.constants import SIGNALS_MAP

from delphi_utils import get_structured_logger

class TestPullNHSNData:
    @patch("delphi_nhsn.pull.Socrata")
    def test_socrata_call(self, mock_socrata, params):
        backup_dir = params["common"]["backup_dir"]
        test_token = params["indicator"]["socrata_token"]
        custom_run = True
        logger = get_structured_logger()

        # Mock Socrata client and its get method
        mock_client = MagicMock()
        mock_socrata.return_value = mock_client
        mock_client.get.side_effect = [[]]

        pull_nhsn_data(test_token, backup_dir, custom_run, logger)

        # Check that Socrata client was initialized with correct arguments
        mock_socrata.assert_called_once_with("data.cdc.gov", test_token)

        # Check that get method was called with correct arguments
        mock_client.get.assert_any_call("ua7e-t2fy", limit=50000, offset=0)

    def test_pull_nhsn_data_output(self, caplog, mock_get, params):
        backup_dir = params["common"]["backup_dir"]
        test_token = params["indicator"]["socrata_token"]
        custom_run = True

        logger = get_structured_logger()

        result = pull_nhsn_data(test_token, backup_dir, custom_run, logger)

        # Check result
        assert result["timestamp"].notnull().all(), "timestamp has rogue NaN"
        assert result["geo_id"].notnull().all(), "geography has rogue NaN"

        # Check for each signal in SIGNALS
        for signal in SIGNALS_MAP.keys():
            assert result[signal].notnull().all(), f"{signal} has rogue NaN"
    def test_pull_nhsn_data_backup(self, mock_get, caplog, params):
        today = pd.Timestamp.today().strftime("%Y%m%d")
        backup_dir = params["common"]["backup_dir"]
        custom_run = params["common"]["custom_run"]
        test_token = params["indicator"]["socrata_token"]

        # Load test data
        expected_data = pd.DataFrame()
        with open("test_data/page.json", "r") as f:
            test_data = json.load(f)
            expected_data = pd.DataFrame(test_data)

        logger = get_structured_logger()
        # Call function with test token
        pull_nhsn_data(test_token, backup_dir, custom_run, logger)

        # Check logger used:
        assert "Backup file created" in caplog.text

        # Check that backup file was created
        backup_files = glob.glob(f"{backup_dir}/{today}*")
        assert len(backup_files) == 2, "Backup file was not created"

        for backup_file in backup_files:
            if backup_file.endswith(".csv.gz"):
                dtypes = expected_data.dtypes.to_dict()
                actual_data = pd.read_csv(backup_file, dtype=dtypes)
            else:
                actual_data = pd.read_parquet(backup_file)
            pd.testing.assert_frame_equal(expected_data, actual_data)

        # clean up
        for file in backup_files:
            os.remove(file)