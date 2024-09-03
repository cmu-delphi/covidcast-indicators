from datetime import datetime, date
import json
import unittest
from unittest.mock import patch, MagicMock
import tempfile
import os
import shutil
import time
from datetime import datetime
import pdb
import pandas as pd
import pandas.api.types as ptypes

from delphi_nssp.pull import (
    get_source_data,
    pull_nssp_data,
)
from delphi_nssp.constants import (
    SIGNALS,
    NEWLINE,
    SIGNALS_MAP,
    TYPE_DICT,
)


class TestPullNSSPData(unittest.TestCase):
    @patch('paramiko.SSHClient')
    def test_get_source_data(self,mock_ssh):
        mock_sftp = MagicMock()
        mock_ssh.return_value.open_sftp.return_value = mock_sftp

        params = {
            "patch": {
                "source_backup_credentials": {
                    "host": "hostname",
                    "user": "user",
                    "path": "/path/to/remote/dir"
                },
                "start_issue": "2021-01-01",
                "end_issue": "2021-01-03",
                "source_dir": "./source_data"
            }
        }
        logger = MagicMock()

        get_source_data(params, logger)

        # Check that the SSH client was used correctly
        mock_ssh.return_value.connect.assert_called_once_with(params["patch"]["source_backup_credentials"]["host"], username=params["patch"]["source_backup_credentials"]["user"])
        mock_ssh.return_value.close.assert_called_once()

        # Check that the SFTP client was used correctly
        mock_sftp.chdir.assert_called_once_with(params["patch"]["source_backup_credentials"]["path"])
        assert mock_sftp.get.call_count == 3  # one call for each date in the range
        mock_sftp.close.assert_called_once()

        shutil.rmtree(params['patch']['source_dir'])

    @patch("delphi_nssp.pull.Socrata")
    def test_pull_nssp_data_for_patch(self, mock_socrata):
        params = {
            "common": {
                "custom_run": True,
            },
            "patch": {
                "current_issue": "2021-01-02",
                "source_dir": "source_dir"
            }
        }
        mock_logger = MagicMock()
        mock_logger.name = "delphi_nssp.patch"
        result = pull_nssp_data("test_token", params, mock_logger)

        # Check that loggger was called with correct info
        mock_logger.info.assert_called_with("Number of records grabbed from source_dir/issue_date.csv",
                                            num_records=len(result),
                                            source="source_dir/2021-01-02.csv")

        test_source_data = pd.read_csv("source_dir/2021-01-02.csv")
        assert test_source_data.shape[0] == result.shape[0] # Check if the number of rows are the same
        mock_socrata.assert_not_called()


    @patch("delphi_nssp.pull.Socrata")
    def test_pull_nssp_data(self, mock_socrata):
        # Load test data
        with open("test_data/page.txt", "r") as f:
            test_data = json.load(f)

        # Mock Socrata client and its get method
        mock_client = MagicMock()
        mock_client.get.side_effect = [test_data, []]  # Return test data on first call, empty list on second call
        mock_socrata.return_value = mock_client

        mock_logger = MagicMock()
        params = { "common": { "custom_run": False } }

        # Call function with test token
        test_token = "test_token"
        result = pull_nssp_data(test_token, params, mock_logger)
        print(result)

        # Check that loggger was called with correct info
        mock_logger.info.assert_called_with("Number of records grabbed from Socrata API",
                                            num_records=len(result),
                                            source="Socrata API")

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


if __name__ == "__main__":
    unittest.main()
