import unittest
from unittest.mock import patch as mock_patch
from delphi_claims_hosp.patch import patch
import os
import shutil

class TestPatchModule:
    def test_patch(self):
        with mock_patch('delphi_claims_hosp.patch.get_structured_logger') as mock_get_structured_logger, \
             mock_patch('delphi_claims_hosp.patch.read_params') as mock_read_params, \
             mock_patch('delphi_claims_hosp.patch.run_module') as mock_run_module:

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

            assert os.path.isdir('./patch_dir')
            assert os.path.isdir('./patch_dir/issue_20210101/hospital-admissions')
            assert os.path.isdir('./patch_dir/issue_20210102/hospital-admissions')

            # Clean up the created directories after the test
            shutil.rmtree(mock_read_params.return_value["patch"]["patch_dir"])