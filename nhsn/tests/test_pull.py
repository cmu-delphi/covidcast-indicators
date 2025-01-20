import glob
from unittest.mock import patch, MagicMock
import os
import pytest

import pandas as pd

from delphi_nhsn.pull import (
    pull_nhsn_data,
    pull_data,
    pull_preliminary_nhsn_data
)
from delphi_nhsn.constants import TYPE_DICT, PRELIM_TYPE_DICT

from delphi_utils import get_structured_logger
from conftest import TEST_DATA, PRELIM_TEST_DATA


DATASETS = [{"id":"ua7e-t2fy",
             "test_data": TEST_DATA},
            {"id":"mpgq-jmmr",
             "test_data":PRELIM_TEST_DATA}
            ]


class TestPullNHSNData:
    @patch("delphi_nhsn.pull.Socrata")
    @pytest.mark.parametrize('dataset', DATASETS, ids=["data", "prelim_data"])
    def test_socrata_call(self, mock_socrata, dataset, params):
        test_token = params["indicator"]["socrata_token"]

        # Mock Socrata client and its get method
        mock_client = MagicMock()
        mock_socrata.return_value = mock_client
        mock_client.get.side_effect = [[]]

        pull_data(test_token, dataset["id"])

        # Check that Socrata client was initialized with correct arguments
        mock_socrata.assert_called_once_with("data.cdc.gov", test_token)

        # Check that get method was called with correct arguments
        mock_client.get.assert_any_call(dataset["id"], limit=50000, offset=0)

    def test_pull_nhsn_data_output(self, caplog, params):
        with patch('sodapy.Socrata.get') as mock_get:
            mock_get.side_effect = [TEST_DATA, []]
            backup_dir = params["common"]["backup_dir"]
            test_token = params["indicator"]["socrata_token"]
            custom_run = True

            logger = get_structured_logger()

            result = pull_nhsn_data(test_token, backup_dir, custom_run, logger)

            expected_columns = set(TYPE_DICT.keys())
            assert set(result.columns) == expected_columns

            for column in list(result.columns):
                assert result[column].notnull().all(), f"{column} has rogue NaN"

    def test_pull_nhsn_data_backup(self, caplog, params):
        with patch('sodapy.Socrata.get') as mock_get:
            mock_get.side_effect = [TEST_DATA, []]

            today = pd.Timestamp.today().strftime("%Y%m%d")
            backup_dir = params["common"]["backup_dir"]
            custom_run = params["common"]["custom_run"]
            test_token = params["indicator"]["socrata_token"]

            # Load test data
            expected_data = pd.DataFrame(TEST_DATA)

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
    def test_pull_prelim_nhsn_data_output(self, caplog, params):
        with patch('sodapy.Socrata.get') as mock_get:
            mock_get.side_effect = [PRELIM_TEST_DATA, []]
            backup_dir = params["common"]["backup_dir"]
            test_token = params["indicator"]["socrata_token"]
            custom_run = True

            logger = get_structured_logger()

            result = pull_preliminary_nhsn_data(test_token, backup_dir, custom_run, logger)

            expected_columns = set(PRELIM_TYPE_DICT.keys())
            assert set(result.columns) == expected_columns

            for column in list(result.columns):
                assert result[column].notnull().all(), f"{column} has rogue NaN"
    def test_pull_prelim_nhsn_data_backup(self, caplog, params):
        with patch('sodapy.Socrata.get') as mock_get:
            mock_get.side_effect = [PRELIM_TEST_DATA, []]

            today = pd.Timestamp.today().strftime("%Y%m%d")
            backup_dir = params["common"]["backup_dir"]
            custom_run = params["common"]["custom_run"]
            test_token = params["indicator"]["socrata_token"]

            # Load test data
            expected_data = pd.DataFrame(PRELIM_TEST_DATA)

            logger = get_structured_logger()
            # Call function with test token
            pull_preliminary_nhsn_data(test_token, backup_dir, custom_run, logger)

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