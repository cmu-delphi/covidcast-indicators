import pytest
from unittest.mock import patch as mock_patch, call, MagicMock
from delphi_nssp.patch import patch, good_patch_config, get_source_data, get_patch_dates
import os
import shutil
from datetime import datetime, timedelta
import pandas as pd

class TestPatchModule:
    @mock_patch('logging.Logger')
    def test_config_missing_custom_run_and_patch_section(self, mock_logger):
        # Case 1: missing custom_run flag and patch section
        patch_config = {
            "common": {}
        }
        assert not good_patch_config(patch_config, mock_logger)
        mock_logger.error.assert_has_calls([
            call("Calling patch.py without custom_run flag set true."),
            call("Custom flag is on, but patch section is missing."),
        ])

    @mock_patch('logging.Logger')
    def test_config_missing_end_issue_in_patch_section(self, mock_logger):
        # Case 2: missing end_issue in patch section
        patch_config = {
            "common": {
                "custom_run": True,
            },
            "patch": {
                "source_dir": "./does_not_exist",
                "source_host": "prod.server.edu",
                "user": "user",
                "patch_dir": "dir",
                "start_issue": "2024-04-21",
            }
        }
        assert not good_patch_config(patch_config, mock_logger)
        mock_logger.error.assert_has_calls([
            call("Patch section is missing required key(s)", missing_keys=["end_issue"]),
        ])

    @mock_patch('logging.Logger')
    def test_config_invalid_start_issue(self, mock_logger):
        # Case 3: start_issue not in yyyy-mm-dd format
        patch_config = {
            "common": {
                "custom_run": True,
            },
            "patch": {
                "source_dir": "./does_not_exist",
                "source_host": "prod.server.edu",
                "user": "user",
                "patch_dir": "dir",
                "start_issue": "01-01-2024",
                "end_issue": "2024-04-22",
            }
        }
        assert not good_patch_config(patch_config, mock_logger)
        mock_logger.error.assert_has_calls([
            call("Issue dates must be in YYYY-MM-DD format.")
        ])

    @mock_patch('logging.Logger')
    def test_config_start_issue_after_end_issue(self, mock_logger):
        # Case 4: start_issue after end_issue
        patch_config = {
            "common": {
                "custom_run": True,
            },
            "patch": {
                "user": "user",
                "patch_dir": "dir",
                "start_issue": "2024-04-22",
                "end_issue": "2024-04-21",
                "source_dir": "./does_not_exist",
                "source_host": "prod.server.edu"
            }
        }
        assert not good_patch_config(patch_config, mock_logger)
        mock_logger.error.assert_called_once_with("Start issue date is after end issue date.")

    @mock_patch('logging.Logger')
    def test_config_all_valid_configurations(self, mock_logger):
        # Case 5: All good configuration
        patch_config = {
            "common": {
                "custom_run": True,
            },
            "patch": {
                "user": "user",
                "patch_dir": "dir",
                "start_issue": "2024-04-21",
                "end_issue": "2024-04-22",
                "source_dir": "./does_not_exist",
                "source_host": "prod.server.edu"
            }
        }
        assert good_patch_config(patch_config, mock_logger)
        mock_logger.info.assert_called_once_with("Good patch configuration.")

    @mock_patch('logging.Logger')
    def test_config_user_and_host_param(self, mock_logger):
        # Case 6.1: pre-existing local source data,
        # so no "user" param in patch section needed
        patch_config = {
            "common": {
                "custom_run": True,
            },
            "patch": {
                "patch_dir": "dir",
                "start_issue": "2024-04-21",
                "end_issue": "2024-04-22",
                "source_dir": "./source_dir"
            }
        }
        assert good_patch_config(patch_config, mock_logger)

        # Case 6.2: source_dir does not exist, so "user" and "source_host" param is needed
        patch_config = {
            "common": {
                "custom_run": True,
            },
            "patch": {
                "patch_dir": "dir",
                "start_issue": "2024-04-21",
                "end_issue": "2024-04-22",
                "source_dir": "./does_not_exist",
            }
        }
        assert not good_patch_config(patch_config, mock_logger)
        mock_logger.error.assert_has_calls([
            call("Patch section is missing required key(s)", missing_keys=["user", "source_host"]),
        ])

    def test_get_patch_dates(self):
        start_issue = datetime(2021, 1, 1)
        end_issue = datetime(2021, 1, 16)
        source_dir = "./source_dir"
        patch_dates = get_patch_dates(start_issue, end_issue, source_dir)
        expected_dates = [
            datetime(2021, 1, 2),
            datetime(2021, 1, 9),
            datetime(2021, 1, 12)
        ]
        print(patch_dates)
        for date in expected_dates:
            assert date in patch_dates

    @mock_patch('delphi_nssp.patch.get_source_data')
    @mock_patch('delphi_nssp.patch.run_module')
    @mock_patch('delphi_nssp.patch.get_structured_logger')
    @mock_patch('delphi_nssp.patch.read_params')
    def test_patch_from_local_source(self, mock_read_params, mock_get_structured_logger, mock_run_module, mock_get_source_data):
        mock_read_params.return_value = {
            "common": {
                "log_filename": "test.log",
                "custom_run": True
            },
            "patch": {
                "user": "user",
                "start_issue": "2021-01-01",
                "end_issue": "2021-01-16",
                "patch_dir": "./patch_dir",
                "source_dir": "./source_dir",
                "source_host": "prod.server.edu"
            }
        }
        patch()

        mock_get_source_data.assert_not_called()

    @mock_patch('delphi_nssp.patch.get_source_data')
    @mock_patch('delphi_nssp.patch.rmtree')
    @mock_patch('delphi_nssp.patch.run_module')
    @mock_patch('delphi_nssp.patch.get_structured_logger')
    @mock_patch('delphi_nssp.patch.read_params')
    def test_patch_download_remote_source(self, mock_read_params, mock_get_structured_logger, mock_run_module, mock_rmtree, mock_get_source_data):
        mock_read_params.return_value = {
            "common": {
                "log_filename": "test.log",
                "custom_run": True
            },
            "patch": {
                "user": "user",
                "start_issue": "2021-01-01",
                "end_issue": "2021-01-16",
                "patch_dir": "./patch_dir",
                "source_host": "prod.server.edu",
                "source_dir": "./does_not_exist"
            }
        }
        patch()
        mock_get_source_data.assert_called_once()
        assert mock_rmtree.call_count == 1
        shutil.rmtree(mock_read_params.return_value["patch"]["patch_dir"])



    @mock_patch('delphi_nssp.patch.get_source_data')
    @mock_patch('delphi_nssp.patch.run_module')
    @mock_patch('delphi_nssp.patch.get_structured_logger')
    @mock_patch('delphi_nssp.patch.read_params')
    def test_patch_confirm_dir_structure_created(self, mock_read_params, mock_get_structured_logger, mock_run_module, mock_get_source_data):
        mock_read_params.return_value = {
            "common": {
                "log_filename": "test.log",
                "custom_run": True
            },
            "patch": {
                "user": "user",
                "start_issue": "2021-01-01",
                "end_issue": "2021-01-16",
                "patch_dir": "./patch_dir",
                "source_dir": "./source_dir",
                "source_host": "prod.server.edu",
            }
        }
        patch()

        # Only Sundays should be issue dirs.
        # Note that data from 2021-01-02.csv falls under issue_20211227.
        issue_dates = ["20201227", "20210103", "20210110"]
        for date_str in issue_dates:
            assert os.path.isdir(f'./patch_dir/issue_{date_str}/nssp')

        not_issue_dates = ["20210101", "20210102", "20210104", "20210105",
                           "20210106", "20210107", "20210108", "20210109",
                           "20210111", "20210112", "20210113", "20210114",
                           "20210115", "20210116"]
        for date_str in not_issue_dates:
            assert not os.path.isdir(f'./patch_dir/issue_{date_str}/nssp')

        # Clean up the created directories after the test
        shutil.rmtree(mock_read_params.return_value["patch"]["patch_dir"])

    @mock_patch('delphi_nssp.patch.get_structured_logger')
    @mock_patch('delphi_nssp.patch.read_params')
    def test_full_patch_code(self, mock_read_params, mock_get_structured_logger):
        mock_get_structured_logger.return_value.name = "delphi_nssp.patch"
        mock_read_params.return_value = {
            "common": {
                "log_filename": "test.log",
                "custom_run": True
            },
            "indicator": {
                "socrata_token": "test_token"
                },
            "patch": {
                "user": "user",
                "start_issue": "2021-01-01",
                "end_issue": "2021-01-16",
                "patch_dir": "./patch_dir",
                "source_dir": "./source_dir",
                "source_host": "prod.server.edu"
            }
        }

        patch()

        # Make sure issue_20210103 has latest weekly data (data from 20210109 instead of 20210108)
        df_20210108 = pd.read_csv('source_dir/20210108.csv.gz')
        df_20210108_nation_combined = df_20210108['percent_visits_combined'].iloc[0]
        df_20210109 = pd.read_csv('source_dir/20210109.csv.gz')
        df_20210109_nation_combined = df_20210109['percent_visits_combined'].iloc[0]
        assert df_20210108_nation_combined != df_20210109_nation_combined

        df_issue_20210103 = pd.read_csv('patch_dir/issue_20210103/nssp/weekly_202040_nation_pct_ed_visits_combined.csv')
        df_issue_20210103_nation_combined = df_issue_20210103['val'].iloc[0]
        assert df_20210109_nation_combined == df_issue_20210103_nation_combined

        # Clean up the created directories after the test
        shutil.rmtree(mock_read_params.return_value["patch"]["patch_dir"])