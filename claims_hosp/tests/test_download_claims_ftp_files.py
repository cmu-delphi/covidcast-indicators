# standard
import datetime
from unittest.mock import MagicMock, patch as mock_patch

# first party
from delphi_claims_hosp.download_claims_ftp_files import (change_date_format,
                                                          download,
                                                          get_timestamp)

FTP_CREDENTIALS = {"host": "test_host", "user": "test_user", "pass": "test_pass", "port": 2222}


def fake_sftp(mock_client, filenames):
    """Stand a mock ftp server up with the given files in ./receiving."""
    sftp = mock_client.return_value.open_sftp.return_value
    sftp.listdir_attr.return_value = [MagicMock(filename=fn) for fn in filenames]
    return sftp


class TestDownloadClaimsFtpFiles:

    def test_change_date_format(self):
        name = "SYNEDI_AGG_INPATIENT_20200611_1451CDT"
        expected = "SYNEDI_AGG_INPATIENT_11062020_1451CDT"
        assert(change_date_format(name)==expected)

        # date and time are no longer always separated
        name = "EDI_AGG_INPATIENT_060620260000CDT.csv.gz"
        expected = "EDI_AGG_INPATIENT_06062026_0000CDT.csv.gz"
        assert(change_date_format(name)==expected)

        name = "EDI_AGG_INPATIENT_082720210251CDT.csv.gz"
        expected = "EDI_AGG_INPATIENT_27082021_0251CDT.csv.gz"
        assert(change_date_format(name)==expected)

        # chunked drops keep the timestamp in the following field
        name = "EDI_AGG_INPATIENT_1_05302020_0352CDT.csv.gz"
        assert(change_date_format(name)==name)

    def test_get_timestamp(self):
        name = "SYNEDI_AGG_INPATIENT_20200611_1451CDT"
        assert(get_timestamp(name).date() == datetime.date(2020, 6, 11))

        name = "EDI_AGG_INPATIENT_08272021_0251CDT.csv.gz.filepart"
        assert(get_timestamp(name).date() == datetime.date(2021, 8, 27))

        name = "EDI_AGG_INPATIENT_1_05302020_0352CDT.csv.gz"
        assert(get_timestamp(name).date() == datetime.date(2020, 5, 30))

        # date and time are no longer always separated
        name = "EDI_AGG_INPATIENT_060620260000CDT.csv.gz"
        assert(get_timestamp(name) == datetime.datetime(2026, 6, 6, 0, 0))

        name = "EDI_AGG_INPATIENT_1_053020200352CDT.csv.gz"
        assert(get_timestamp(name).date() == datetime.date(2020, 5, 30))

        name = "EDI_AGG_INPATIENT_no_date_here.csv.gz"
        assert(get_timestamp(name) == datetime.datetime(1900, 1, 1))

    @mock_patch("delphi_claims_hosp.download_claims_ftp_files.paramiko.SSHClient")
    def test_download_for_an_issue_date_takes_only_that_days_drops(self, mock_client, tmp_path):
        sftp = fake_sftp(mock_client, [
            "EDI_AGG_INPATIENT_20200610_1451CDT.csv.gz",
            "EDI_AGG_INPATIENT_20200611_0251CDT.csv.gz",
            "EDI_AGG_INPATIENT_20200611_1451CDT.csv.gz",
            "EDI_AGG_INPATIENT_20200612_1451CDT.csv.gz",
        ])

        download(FTP_CREDENTIALS, str(tmp_path), MagicMock(), issue_date="2020-06-11")

        # neighbouring days are left alone
        assert sorted(call.args[0] for call in sftp.get.call_args_list) == [
            "EDI_AGG_INPATIENT_20200611_0251CDT.csv.gz",
            "EDI_AGG_INPATIENT_20200611_1451CDT.csv.gz",
        ]

    @mock_patch("delphi_claims_hosp.download_claims_ftp_files.paramiko.SSHClient")
    def test_download_without_an_issue_date_keeps_the_rolling_window(self, mock_client, tmp_path):
        now = datetime.datetime.now()
        recent = (now - datetime.timedelta(hours=2)).strftime("EDI_AGG_INPATIENT_%Y%m%d_%H%M") + "CDT.csv.gz"
        stale = (now - datetime.timedelta(days=3)).strftime("EDI_AGG_INPATIENT_%Y%m%d_%H%M") + "CDT.csv.gz"
        sftp = fake_sftp(mock_client, [recent, stale])

        download(FTP_CREDENTIALS, str(tmp_path), MagicMock())

        assert [call.args[0] for call in sftp.get.call_args_list] == [recent]
