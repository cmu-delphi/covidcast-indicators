# standard
import datetime
import re
from unittest.mock import MagicMock, patch
import logging

from delphi_claims_hosp.download_claims_ftp_files import (change_date_format,
                                                          get_timestamp, download)

OLD_FILENAME_TIMESTAMP = re.compile(
    r".*EDI_AGG_INPATIENT_[0-9]_(?P<ymd>[0-9]*)_(?P<hm>[0-9]*)[^0-9]*")
NEW_FILENAME_TIMESTAMP = re.compile(r".*EDI_AGG_INPATIENT_(?P<ymd>[0-9]*)_(?P<hm>[0-9]*)[^0-9]*")

TEST_LOGGER = logging.getLogger()
class TestDownloadClaimsFtpFiles:

    @patch('delphi_claims_hosp.download_claims_ftp_files.paramiko.SSHClient')
    @patch('delphi_claims_hosp.download_claims_ftp_files.path.exists', return_value=False)
    def test_download(self, mock_exists, mock_sshclient):
        mock_sshclient_instance = MagicMock()
        mock_sshclient.return_value = mock_sshclient_instance
        mock_sftp = MagicMock()
        mock_sshclient_instance.open_sftp.return_value = mock_sftp
        mock_sftp.listdir_attr.return_value = [MagicMock(filename="SYNEDI_AGG_INPATIENT_11062020_1451CDT.csv.gz")]
        ftp_credentials = {"host": "test_host", "user": "test_user", "pass": "test_pass", "port": "test_port"}
        out_path = "./test_data/"

        issue_date = datetime.datetime(2020, 11, 7)
        download(ftp_credentials, out_path, TEST_LOGGER, issue_date=issue_date)
        mock_sshclient_instance.connect.assert_called_once_with(ftp_credentials["host"], username=ftp_credentials["user"], password=ftp_credentials["pass"], port=ftp_credentials["port"])
        mock_sftp.get.assert_called()

    
    def test_change_date_format(self):
        name = "SYNEDI_AGG_INPATIENT_20200611_1451CDT"
        expected = "SYNEDI_AGG_INPATIENT_11062020_1451CDT"
        assert(change_date_format(name)==expected)
        
    def test_get_timestamp(self):
        name = "SYNEDI_AGG_INPATIENT_20200611_1451CDT"
        assert(get_timestamp(name).date() == datetime.date(2020, 6, 11))
        
        name = "EDI_AGG_INPATIENT_08272021_0251CDT.csv.gz.filepart"
        assert(get_timestamp(name).date() == datetime.date(2021, 8, 27))
        
        name = "EDI_AGG_INPATIENT_1_05302020_0352CDT.csv.gz"
        assert(get_timestamp(name).date() == datetime.date(2020, 5, 30))
