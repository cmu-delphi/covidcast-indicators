import unittest
from unittest.mock import patch as mock_patch, call
from delphi_nssp.patch import patch, good_patch_config
import os
import shutil
from datetime import datetime, timedelta

class TestPatchModule(unittest.TestCase):
    def test_good_patch_config(self):
        # Case 1: missing custom_run flag and patch section
        with mock_patch('logging.Logger') as mock_logger:
            patch_config = {
                "common": {}
            }
            self.assertFalse(good_patch_config(patch_config, mock_logger))
            mock_logger.error.assert_has_calls([
                call("Calling patch.py without custom_run flag set true."),
                call("Custom flag is on, but patch section is missing."),
            ])

        # Case 2: missing end_issue in patch section
        with mock_patch('logging.Logger') as mock_logger:
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
            self.assertFalse(good_patch_config(patch_config, mock_logger))
            mock_logger.error.assert_has_calls([
                call("Patch section is missing required key(s): end_issue"),
            ])

        # Case 3: start_issue not in yyyy-mm-dd format and source_dir doesn't exist.
        with mock_patch('logging.Logger') as mock_logger:
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
            self.assertFalse(good_patch_config(patch_config, mock_logger))
            mock_logger.error.assert_has_calls([
                call("Issue dates must be in YYYY-MM-DD format."),
                call("Source directory bad_source_dir does not exist.")
            ])

        # Case 4: start_issue after end_issue
        with mock_patch('logging.Logger') as mock_logger:
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
            self.assertFalse(good_patch_config(patch_config, mock_logger))
            mock_logger.error.assert_called_once_with("Start issue date is after end issue date.")

        # Case 5: All good configuration
        with mock_patch('logging.Logger') as mock_logger:
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
            self.assertTrue(good_patch_config(patch_config, mock_logger))
            mock_logger.info.assert_called_once_with("Good patch configuration.")



    def test_patch(self):
        with mock_patch('delphi_nssp.patch.run_module') as mock_run_module, \
             mock_patch('delphi_nssp.patch.get_structured_logger') as mock_get_structured_logger, \
             mock_patch('delphi_nssp.patch.read_params') as mock_read_params:

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

            self.assertIn('current_issue', mock_read_params.return_value['patch'])
            self.assertEqual(mock_read_params.return_value['patch']['current_issue'], '2021-01-02')

            self.assertTrue(os.path.isdir('./patch_dir'))
            self.assertTrue(os.path.isdir('./patch_dir/issue_20201227/nssp'))

            start_date = datetime(2020, 12, 28)
            end_date = datetime(2021, 1, 3)
            date = start_date

            while date <= end_date:
                date_str = date.strftime("%Y%m%d")
                self.assertFalse(os.path.isdir(f'./patch_dir/issue_{date_str}/nssp'))
                date += timedelta(days=1)

            # Clean up the created directories after the test
            shutil.rmtree(mock_read_params.return_value["patch"]["patch_dir"])

if __name__ == '__main__':
    unittest.main()