import glob
from unittest.mock import patch, MagicMock
import os
import pytest

import pandas as pd

from delphi_nhsn.pull import (
    pull_nhsn_data,
    pull_data,
    pull_preliminary_nhsn_data, pull_data_from_file
)
from delphi_nhsn.constants import SIGNALS_MAP, PRELIM_SIGNALS_MAP

from delphi_utils import get_structured_logger
from conftest import TEST_DATA, PRELIM_TEST_DATA, TEST_DIR

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

    def test_pull_from_file(self, caplog, params_w_patch):
        backup_dir = f"{TEST_DIR}/test_data"
        issue_date = params_w_patch["patch"]["issue_date"]
        logger = get_structured_logger()

        # Load test data
        expected_data = pd.DataFrame(TEST_DATA)

        df = pull_data_from_file(backup_dir, issue_date, logger=logger)
        df = df.astype('str')
        expected_data = expected_data.astype('str')
        assert "Pulling data from file" in caplog.text

        pd.testing.assert_frame_equal(expected_data, df)

    def test_pull_from_file_prelim(self, caplog, params_w_patch):
        backup_dir = f"{TEST_DIR}/test_data"
        issue_date = params_w_patch["patch"]["issue_date"]
        logger = get_structured_logger()

        # Load test data
        expected_data = pd.DataFrame(PRELIM_TEST_DATA)

        df = pull_data_from_file(backup_dir, issue_date, logger=logger, prelim_flag=True)
        df = df.astype('str')
        expected_data = expected_data.astype('str')

        assert "Pulling data from file" in caplog.text
        pd.testing.assert_frame_equal(expected_data, df)

    def test_pull_nhsn_data_output(self, caplog, params):
        with patch('sodapy.Socrata.get') as mock_get:
            mock_get.side_effect = [TEST_DATA, []]
            backup_dir = params["common"]["backup_dir"]
            test_token = params["indicator"]["socrata_token"]
            custom_run = params["common"]["custom_run"]

            logger = get_structured_logger()

            result = pull_nhsn_data(test_token, backup_dir, custom_run, issue_date=None, logger=logger)

            # Check result
            assert result["timestamp"].notnull().all(), "timestamp has rogue NaN"
            assert result["geo_id"].notnull().all(), "geography has rogue NaN"

            # Check for each signal in SIGNALS
            for signal in SIGNALS_MAP.keys():
                assert result[signal].notnull().all(), f"{signal} has rogue NaN"
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
            pull_nhsn_data(test_token, backup_dir, custom_run, issue_date=None, logger=logger)

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
            custom_run = params["common"]["custom_run"]

            logger = get_structured_logger()

            result = pull_preliminary_nhsn_data(test_token, backup_dir, custom_run, issue_date=None, logger=logger)

            # Check result
            assert result["timestamp"].notnull().all(), "timestamp has rogue NaN"
            assert result["geo_id"].notnull().all(), "geography has rogue NaN"

            # Check for each signal in SIGNALS
            for signal in PRELIM_SIGNALS_MAP.keys():
                assert result[signal].notnull().all(), f"{signal} has rogue NaN"
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
            pull_preliminary_nhsn_data(test_token, backup_dir, custom_run, issue_date=None, logger=logger)

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