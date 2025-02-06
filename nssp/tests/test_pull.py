import glob
import json
from unittest.mock import patch, MagicMock
import os
import shutil
import time
from datetime import datetime
import pdb
import pandas as pd

from delphi_nssp.pull import (
    get_source_data,
    pull_nssp_data,
    pull_with_socrata_api,
)

from delphi_nssp.constants import (
    NEWLINE,
    SIGNALS,
    SIGNALS_MAP,
    TYPE_DICT,
)

from delphi_utils import get_structured_logger

class TestPullNSSPData:
    @patch("delphi_nssp.pull.Socrata")
    def test_pull_nssp_data(self, mock_socrata, caplog):
        today = pd.Timestamp.today().strftime("%Y%m%d")
        backup_dir = 'test_raw_data_backups'

        # Load test data
        with open("test_data/page.txt", "r") as f:
            test_data = json.load(f)

        # Mock Socrata client and its get method
        mock_client = MagicMock()
        mock_client.get.side_effect = [test_data, []]  # Return test data on first call, empty list on second call
        mock_socrata.return_value = mock_client

        custom_run = False
        logger = get_structured_logger()
        # Call function with test token
        test_token = "test_token"
        result = pull_nssp_data(test_token, backup_dir, custom_run, logger=logger)

        # Check logger used:
        assert "Backup file created" in caplog.text

        # Check that backup file was created
        backup_files = glob.glob(f"{backup_dir}/{today}.*")
        assert len(backup_files) == 2, "Backup file was not created"

        expected_data = pd.DataFrame(test_data)
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

if __name__ == "__main__":
    unittest.main()
