import unittest
from unittest.mock import patch as mock_patch, call
from delphi_nssp.patch import patch
import os
import shutil
from datetime import datetime, timedelta

class TestPatchModule(unittest.TestCase):
    def test_patch(self):
        with mock_patch('delphi_nssp.patch.run_module') as mock_run_module, \
             mock_patch('delphi_nssp.patch.get_structured_logger') as mock_get_structured_logger, \
             mock_patch('delphi_nssp.patch.read_params') as mock_read_params:

            mock_read_params.return_value = {
                "common": {
                    "log_filename": "test.log"
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