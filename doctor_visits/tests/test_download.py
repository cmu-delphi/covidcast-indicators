import unittest
from unittest.mock import patch, MagicMock
from delphi_doctor_visits.download_claims_ftp_files import download

class TestDownload(unittest.TestCase):
    @patch('delphi_doctor_visits.download_claims_ftp_files.paramiko.SSHClient')
    @patch('delphi_doctor_visits.download_claims_ftp_files.path.exists', return_value=False)
    def test_download(self, mock_exists, mock_sshclient):
        mock_sshclient_instance = MagicMock()
        mock_sshclient.return_value = mock_sshclient_instance
        mock_sftp = MagicMock()
        mock_sshclient_instance.open_sftp.return_value = mock_sftp
        mock_sftp.listdir_attr.return_value = [MagicMock(filename="SYNEDI_AGG_OUTPATIENT_20200207_1455CDT.csv.gz")]
        ftp_credentials = {"host": "test_host", "user": "test_user", "pass": "test_pass", "port": "test_port"}
        out_path = "./test_data/"
        logger = MagicMock()

        #case 1: download with issue_date that does not exist on ftp server
        download(ftp_credentials, out_path, logger, issue_date="2020-02-08")
        mock_sshclient_instance.connect.assert_called_once_with(ftp_credentials["host"], username=ftp_credentials["user"], password=ftp_credentials["pass"], port=ftp_credentials["port"])
        mock_sftp.get.assert_not_called()

        # case 2: download with issue_date that exists on ftp server
        download(ftp_credentials, out_path, logger, issue_date="2020-02-07")
        mock_sftp.get.assert_called()

if __name__ == '__main__':
    unittest.main()