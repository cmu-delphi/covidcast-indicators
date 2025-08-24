from pathlib import Path
from unittest.mock import patch, MagicMock
import shutil

import pytest
from delphi_claims_hosp.download_claims_ftp_files import change_date_format

from delphi_claims_hosp.run import run_module
from delphi_claims_hosp.backfill import merge_existing_backfill_files, merge_backfill_file
from freezegun import freeze_time

from conftest import TEST_DIR
class TestRun:
    @freeze_time("2020-06-11 20:00:00")
    def test_output_files(self, params):
        with patch('delphi_claims_hosp.patch.get_structured_logger'), \
            patch('delphi_claims_hosp.download_claims_ftp_files.paramiko.SSHClient') as mock_ssh_client, \
            patch('delphi_claims_hosp.download_claims_ftp_files.path.exists', return_value=False), \
            patch('delphi_claims_hosp.run.merge_existing_backfill_files') as mock_patch_backfill, \
            patch('delphi_claims_hosp.run.merge_backfill_file') as mock_backfill:

            mock_ssh_client_instance = MagicMock()
            mock_ssh_client.return_value = mock_ssh_client_instance
            mock_sftp = MagicMock()
            mock_ssh_client_instance.open_sftp.return_value = mock_sftp
            mock_sftp.listdir_attr.return_value = [MagicMock(filename="SYNEDI_AGG_INPATIENT_20200611_1451CDT.csv.gz")]
            def mock_get(*args, **kwargs):
                file = change_date_format(args[0])
                src = Path(f"{TEST_DIR}/test_data/{file}")
                dst = Path(f"{TEST_DIR}/receiving/{file}")
                shutil.copyfile(src, dst)
            mock_sftp.get.side_effect = mock_get

            mock_patch_backfill.side_effect = merge_existing_backfill_files
            mock_backfill.side_effect = merge_backfill_file

            run_module(params)

            assert mock_patch_backfill.call_count == 0
            assert mock_backfill.call_count == 1

        # Clean up the created directories after the test
        shutil.rmtree(params["common"]["export_dir"])
