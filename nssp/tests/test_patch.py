import pytest
from unittest.mock import patch as mock_patch, call
from delphi_nssp.patch import patch, good_patch_config
import os
import shutil
from datetime import datetime, timedelta
import pandas as pd

class TestPatchModule:
    @mock_patch('logging.Logger')
    def test_good_patch_config_case1(self, mock_logger):
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
    def test_good_patch_config_case2(self, mock_logger):
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
            call("Patch section is missing required key(s): end_issue"),
        ])

    @mock_patch('logging.Logger')
    def test_good_patch_config_case3(self, mock_logger):
        # Case 3: start_issue not in yyyy-mm-dd format and source_dir doesn't exist.
        patch_config = {
            "common": {
                "custom_run": True,
            },
            "patch": {
                "patch_dir": "dir",
                "start_issue": "01-01-2024",
                "end_issue": "2024-04-22",
                "source_dir": "bad_source_dir"
            }
        }
        assert not good_patch_config(patch_config, mock_logger)
        mock_logger.error.assert_has_calls([
            call("Issue dates must be in YYYY-MM-DD format."),
            call("Source directory bad_source_dir does not exist.")
        ])

    @mock_patch('logging.Logger')
    def test_good_patch_config_case4(self, mock_logger):
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
    def test_good_patch_config_case5(self, mock_logger):
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
    def test_patch(self, mock_read_params, mock_get_structured_logger, mock_run_module):
        mock_read_params.return_value = {
            "common": {
                "log_filename": "test.log",
                "custom_run": True
            },
            "patch": {
                "start_issue": "2021-01-01",
                "end_issue": "2021-01-02",
                "patch_dir": "./patch_dir",
                "source_dir": "./source_dir"
            }
        }

        patch()

        assert 'current_issue' in mock_read_params.return_value['patch']
        assert mock_read_params.return_value['patch']['current_issue'] == '2021-01-02'

        assert os.path.isdir('./patch_dir')
        assert os.path.isdir('./patch_dir/issue_20201227/nssp')

        start_date = datetime(2020, 12, 28)
        end_date = datetime(2021, 1, 3)
        date = start_date

        while date <= end_date:
            date_str = date.strftime("%Y%m%d")
            assert not os.path.isdir(f'./patch_dir/issue_{date_str}/nssp')
            date += timedelta(days=1)

        # Clean up the created directories after the test
        shutil.rmtree(mock_read_params.return_value["patch"]["patch_dir"])

    # @mock_patch('delphi_nssp.patch.run_module')
    @mock_patch('delphi_nssp.patch.get_structured_logger')
    @mock_patch('delphi_nssp.patch.read_params')
    def test_patch(self, mock_read_params, mock_get_structured_logger):
    # def test_patch(self, mock_read_params, mock_get_structured_logger, mock_run_module):
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

        start_date = datetime(2021, 1, 1)
        end_date = datetime(2021, 1, 16)
        date = start_date

        # Only Sundays should be issue dirs.
        while date <= end_date:
            date_str = date.strftime("%Y%m%d")
            if date.weekday() == 6:
                assert os.path.isdir(f'./patch_dir/issue_{date_str}/nssp')
            else:
                assert not os.path.isdir(f'./patch_dir/issue_{date_str}/nssp')
            date += timedelta(days=1)

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