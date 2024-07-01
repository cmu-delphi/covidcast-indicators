import unittest
from unittest.mock import patch as mock_patch, call
from delphi_doctor_visits.patch import patch
import os
import shutil

class TestPatchModule(unittest.TestCase):
    def test_patch(self):
        with mock_patch('delphi_doctor_visits.patch.run_module') as mock_run_module, \
             mock_patch('delphi_doctor_visits.patch.get_structured_logger') as mock_get_structured_logger, \
             mock_patch('delphi_doctor_visits.patch.read_params') as mock_read_params:

            mock_read_params.return_value = {
                "common": {
                    "log_filename": "test.log"
                },
                "patch": {
                    "start_issue": "2021-01-01",
                    "end_issue": "2021-01-02",
                    "patch_dir": "./patch_dir"
                }
            }

            patch()

            self.assertIn('current_issue', mock_read_params.return_value)
            self.assertEqual(mock_read_params.return_value['current_issue'], '2021-01-02')

            self.assertTrue(os.path.isdir('./patch_dir'))
            self.assertTrue(os.path.isdir('./patch_dir/issue_20210101/doctor-visits'))
            self.assertTrue(os.path.isdir('./patch_dir/issue_20210102/doctor-visits'))

            # Clean up the created directories after the test
            shutil.rmtree(mock_read_params.return_value["patch"]["patch_dir"])

if __name__ == '__main__':
    unittest.main()