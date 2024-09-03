import pytest
from unittest.mock import patch as mock_patch, call, MagicMock
from delphi_nssp.patch import patch, good_patch_config, get_source_data
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
                "patch_dir": "dir",
                "start_issue": "2024-04-21",
                "source_dir": "source_dir"
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
                "patch_dir": "dir",
                "start_issue": "01-01-2024",
                "end_issue": "2024-04-22",
                "source_dir": "source_dir"
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
                "patch_dir": "dir",
                "start_issue": "2024-04-22",
                "end_issue": "2024-04-21",
                "source_dir": "source_dir"
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
                "patch_dir": "dir",
                "start_issue": "2024-04-21",
                "end_issue": "2024-04-22",
                "source_dir": "source_dir"
            }
        }
        assert good_patch_config(patch_config, mock_logger)
        mock_logger.info.assert_called_once_with("Good patch configuration.")

    @mock_patch('delphi_nssp.patch.run_module')
    @mock_patch('delphi_nssp.patch.get_structured_logger')
    @mock_patch('delphi_nssp.patch.read_params')
    def test_patch_confirm_dir_structure_created(self, mock_read_params, mock_get_structured_logger, mock_run_module):
        mock_read_params.return_value = {
            "common": {
                "log_filename": "test.log",
                "custom_run": True
            },
            "patch": {
                "start_issue": "2021-01-01",
                "end_issue": "2021-01-16",
                "patch_dir": "./patch_dir",
                "source_dir": "./source_dir"
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
    def test_patch_confirm_values_generated(self, mock_read_params, mock_get_structured_logger):
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
                "start_issue": "2021-01-01",
                "end_issue": "2021-01-16",
                "patch_dir": "./patch_dir",
                "source_dir": "./source_dir"
            }
        }

        patch()

        # Make sure issue_20210103 has latest weekly data (data from 20210109 instead of 20210108)
        df_20210108 = pd.read_csv('source_dir/2021-01-08.csv')
        df_20210108_nation_combined = df_20210108['percent_visits_combined'].iloc[0]
        df_20210109 = pd.read_csv('source_dir/2021-01-09.csv')
        df_20210109_nation_combined = df_20210109['percent_visits_combined'].iloc[0]
        assert df_20210108_nation_combined != df_20210109_nation_combined

        df_issue_20210103 = pd.read_csv('patch_dir/issue_20210103/nssp/weekly_202040_nation_pct_ed_visits_combined.csv')
        df_issue_20210103_nation_combined = df_issue_20210103['val'].iloc[0]
        assert df_20210109_nation_combined == df_issue_20210103_nation_combined

        # Clean up the created directories after the test
        shutil.rmtree(mock_read_params.return_value["patch"]["patch_dir"])