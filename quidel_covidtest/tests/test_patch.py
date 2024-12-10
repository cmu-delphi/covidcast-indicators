from unittest.mock import patch as mock_patch
from delphi_quidel_covidtest.patch import patch
import os
import shutil

class TestPatchModule:
    def test_patch(self):
        with mock_patch('delphi_quidel_covidtest.patch.get_structured_logger') as mock_get_structured_logger, \
             mock_patch('delphi_quidel_covidtest.patch.read_params') as mock_read_params, \
             mock_patch('delphi_quidel_covidtest.patch.run_module') as mock_run_module:

            mock_read_params.return_value = {
                "common": {
                    "log_filename": "test.log"
                },
                "indicator": {
                    "export_day_range": 40,
                },
                "patch": {
                    "start_issue": "2021-01-01",
                    "end_issue": "2021-01-02",
                    "patch_dir": "./patch_dir"
                }
            }

            patch()

            assert os.path.isdir('./patch_dir')
            assert os.path.isdir('./patch_dir/issue_20210101/quidel-covidtest')
            assert os.path.isdir('./patch_dir/issue_20210102/quidel-covidtest')

            # Clean up the created directories after the test
            shutil.rmtree(mock_read_params.return_value["patch"]["patch_dir"])