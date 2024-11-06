import json
from unittest.mock import patch, MagicMock
import os

from delphi_nssp.pull import (
    pull_nssp_data,
)
from delphi_nssp.constants import SIGNALS

from delphi_utils import get_structured_logger

class TestPullNSSPData:
    @patch("delphi_nssp.pull.Socrata")
    def test_pull_nssp_data(self, mock_socrata, caplog):
        # Load test data
        with open("test_data/page.txt", "r") as f:
            test_data = json.load(f)

        # Mock Socrata client and its get method
        mock_client = MagicMock()
        mock_client.get.side_effect = [test_data, []]  # Return test data on first call, empty list on second call
        mock_socrata.return_value = mock_client

        backup_dir = 'test_raw_data_backups'
        # Clear old backup files
        for file in os.listdir(backup_dir):
            os.remove(os.path.join(backup_dir, file))

        custom_run = False

        logger = get_structured_logger()

        # Call function with test token
        test_token = "test_token"
        result = pull_nssp_data(test_token, backup_dir, custom_run, logger)

        # Check logger used:
        assert "Backup file created" in caplog.text

        # Check that backup file was created
        backup_files = os.listdir(backup_dir)
        assert len(backup_files) == 2, "Backup file was not created"

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

