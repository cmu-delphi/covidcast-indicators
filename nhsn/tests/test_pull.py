import glob
import time
from unittest.mock import patch, MagicMock
import os
import pytest
from urllib.error import HTTPError
import pandas as pd

from delphi_nhsn.pull import (
    pull_nhsn_data,
    pull_data,
    pull_data_from_file,
    check_last_updated
)
from delphi_nhsn.constants import TYPE_DICT, PRELIM_TYPE_DICT, PRELIM_DATASET_ID, MAIN_DATASET_ID

from delphi_utils import get_structured_logger
from conftest import TEST_DATA, PRELIM_TEST_DATA, TEST_DIR

DATASETS = [{"id":MAIN_DATASET_ID,
             "test_data": TEST_DATA,
             "msg_prefix": "",
             "prelim_flag": False,
             "expected_data": f"{TEST_DIR}/test_data/expected_df.csv",
             "type_dict": TYPE_DICT,
             },

            {"id":PRELIM_DATASET_ID,
             "test_data":PRELIM_TEST_DATA,
             "msg_prefix": "Preliminary ",
             "prelim_flag": True,
             "expected_data": f"{TEST_DIR}/test_data/expected_df_prelim.csv",
             "type_dict": PRELIM_TYPE_DICT,
             }
            ]


class TestPullNHSNData:
    @patch("delphi_nhsn.pull.Socrata")
    @pytest.mark.parametrize('dataset', DATASETS, ids=["data", "prelim_data"])
    def test_socrata_call(self, mock_socrata, dataset, params):
        test_token = params["indicator"]["socrata_token"]
        backup_dir = f"{TEST_DIR}/test_data"
        logger = get_structured_logger()

        # Mock Socrata client and its get method
        mock_client = MagicMock()
        mock_socrata.return_value = mock_client
        # testing retry behavior
        http_error = HTTPError(url="", hdrs="", fp="", msg="Service Temporarily Unavailable",code=503)
        mock_client.get.side_effect = [http_error,[]]

        pull_data(test_token, dataset["id"], backup_dir, logger)

        # Check that Socrata client was initialized with correct arguments
        mock_socrata.assert_called_once_with("data.cdc.gov", test_token)

        # Check that get method was called with correct arguments
        mock_client.get.assert_any_call(dataset["id"], limit=50000, offset=0)

    @pytest.mark.parametrize('dataset', DATASETS, ids=["data", "prelim_data"])
    def test_pull_from_file(self, caplog, dataset, params_w_patch):
        backup_dir = f"{TEST_DIR}/test_data"
        issue_date = params_w_patch["patch"]["issue_date"]
        logger = get_structured_logger()
        prelim_flag = dataset["prelim_flag"]
        # Load test data
        expected_data = pd.DataFrame(dataset["test_data"])

        df = pull_data_from_file(backup_dir, issue_date, logger=logger, prelim_flag=prelim_flag)

        # expected_data reads from dictionary and defaults all the columns as object data types
        # compared to the method which pd.read_csv somewhat interprets numerical data types
        expected_data = expected_data.astype(df.dtypes.to_dict())
        # expected_data = expected_data.astype('str')
        assert "Pulling data from file" in caplog.text

        pd.testing.assert_frame_equal(expected_data, df)

    @patch("delphi_nhsn.pull.Socrata")
    @patch("delphi_nhsn.pull.create_backup_csv")
    @pytest.mark.parametrize('dataset', DATASETS, ids=["data", "prelim_data"])
    def test_pull_nhsn_data_output(self, mock_create_backup, mock_socrata, dataset, caplog, params):
        now = time.time()
        # Mock Socrata client and its get method
        mock_client = MagicMock()
        mock_socrata.return_value = mock_client
        mock_client.get.side_effect = [dataset["test_data"],[]]
        mock_client.get_metadata.return_value = {"rowsUpdatedAt": now}

        backup_dir = params["common"]["backup_dir"]
        test_token = params["indicator"]["socrata_token"]
        custom_run = params["common"]["custom_run"]
        logger = get_structured_logger()

        expected_df = pd.read_csv(dataset["expected_data"])

        result = pull_nhsn_data(test_token, backup_dir, custom_run, issue_date=None, logger=logger, preliminary=dataset["prelim_flag"])
        mock_create_backup.assert_called_once()

        expected_columns = set(expected_df.columns)
        assert set(result.columns) == expected_columns

        for column in list(result.columns):
            # some states don't report confirmed admissions rsv
            if column == "confirmed_admissions_rsv_ew" and not dataset["prelim_flag"]:
                continue
            if column == "confirmed_admissions_rsv_ew_prelim" and dataset["prelim_flag"]:
                continue
            assert result[column].notnull().all(), f"{column} has rogue NaN"

        expected_df = expected_df.astype(dataset["type_dict"])

        pd.testing.assert_frame_equal(expected_df, result)


    @patch("delphi_nhsn.pull.Socrata")
    @pytest.mark.parametrize('dataset', DATASETS, ids=["data", "prelim_data"])
    def test_pull_nhsn_data_backup(self, mock_socrata, dataset, caplog, params):
        now = time.time()
        # Mock Socrata client and its get method
        mock_client = MagicMock()
        mock_socrata.return_value = mock_client
        mock_client.get.side_effect = [dataset["test_data"], []]

        mock_client.get_metadata.return_value = {"viewLastModified": now}

        today = pd.Timestamp.today().strftime("%Y%m%d")
        backup_dir = params["common"]["backup_dir"]
        custom_run = params["common"]["custom_run"]
        test_token = params["indicator"]["socrata_token"]

        # Load test data
        expected_data = pd.DataFrame(dataset["test_data"])

        logger = get_structured_logger()
        # Call function with test token
        pull_nhsn_data(test_token, backup_dir, custom_run, issue_date=None, logger=logger, preliminary=dataset["prelim_flag"])

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


    @pytest.mark.parametrize('dataset', DATASETS, ids=["data", "prelim_data"])
    @pytest.mark.parametrize("updatedAt", [time.time(), time.time() - 172800], ids=["updated", "stale"])
    @patch("delphi_nhsn.pull.Socrata")
    def test_check_last_updated(self, mock_socrata, dataset, updatedAt, caplog):
        mock_client = MagicMock()
        mock_socrata.return_value = mock_client
        mock_client.get_metadata.return_value = {"viewLastModified": updatedAt }
        logger = get_structured_logger()

        check_last_updated(mock_client, dataset["id"], logger)

        # Check that get method was called with correct arguments
        now = time.time()
        if now - updatedAt < 60:
            assert f"{dataset['msg_prefix']}NHSN data was recently updated; Pulling data" in caplog.text
        else:
            stale_msg = f"{dataset['msg_prefix']}NHSN data is stale; Skipping"
            assert stale_msg in caplog.text

