import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch as mock_patch, MagicMock
import os
import shutil
from conftest import TEST_DIR

from delphi_claims_hosp.patch import patch
from delphi_claims_hosp.backfill import merge_existing_backfill_files

class TestPatchModule:

    def test_patch(self, params_w_patch):
        with mock_patch('delphi_claims_hosp.patch.get_structured_logger'), \
            mock_patch('delphi_claims_hosp.patch.read_params') as mock_read_params, \
            mock_patch('delphi_claims_hosp.download_claims_ftp_files.paramiko.SSHClient') as mock_ssh_client, \
            mock_patch('delphi_claims_hosp.download_claims_ftp_files.path.exists', return_value=False), \
            mock_patch('delphi_claims_hosp.backfill.merge_existing_backfill_files') as mock_backfill:
            mock_ssh_client_instance = MagicMock()
            mock_ssh_client.return_value = mock_ssh_client_instance
            mock_sftp = MagicMock()
            mock_ssh_client_instance.open_sftp.return_value = mock_sftp
            mock_sftp.listdir_attr.return_value = [MagicMock(filename="SYNEDI_AGG_INPATIENT_11062020_1451CDT.csv.gz")]
            def mock_get(*args, **kwargs):
                src = Path(f"{TEST_DIR}/test_data/{args[0]}")
                dst = Path(f"{TEST_DIR}/receiving/{args[0]}")
                shutil.copyfile(src, dst)
            mock_sftp.get.side_effect = mock_get

            mock_read_params.return_value = params_w_patch
            mock_backfill.side_effect = merge_existing_backfill_files

            patch()

            # assert mock_backfill.assert_called()

            issue_date = params_w_patch["patch"]["start_issue"].replace("-", "")
            assert os.path.isdir(f'{TEST_DIR}/patch_dir/issue_{issue_date}/hospital-admissions')

            # Clean up the created directories after the test
            shutil.rmtree(mock_read_params.return_value["patch"]["patch_dir"])