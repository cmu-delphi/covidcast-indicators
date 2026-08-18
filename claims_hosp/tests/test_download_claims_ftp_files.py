# standard
import datetime

# third party
import pytest

# first party
from delphi_claims_hosp.download_claims_ftp_files import (change_date_format,
                                                          get_timestamp)


class TestDownloadClaimsFtpFiles:

    @pytest.mark.parametrize("name, expected", [
        # YYYYMMDD, separated
        ("SYNEDI_AGG_INPATIENT_20200611_1451CDT",
         "SYNEDI_AGG_INPATIENT_11062020_1451CDT"),
        # MMDDYYYY, separated
        ("EDI_AGG_INPATIENT_08272021_0251CDT.csv.gz",
         "EDI_AGG_INPATIENT_27082021_0251CDT.csv.gz"),
        # MMDDYYYY, no separator between date and time
        ("EDI_AGG_INPATIENT_060620260000CDT.csv.gz",
         "EDI_AGG_INPATIENT_06062026_0000CDT.csv.gz"),
        # chunked drops keep their chunk number
        ("EDI_AGG_INPATIENT_1_05302020_0352CDT.csv.gz",
         "EDI_AGG_INPATIENT_1_30052020_0352CDT.csv.gz"),
        # unparseable names are passed through untouched
        ("EDI_AGG_INPATIENT_nonsense.csv.gz",
         "EDI_AGG_INPATIENT_nonsense.csv.gz"),
    ])
    def test_change_date_format(self, name, expected):
        assert change_date_format(name) == expected

    def test_change_date_format_is_readable_downstream(self):
        """get_latest_filename splits on _ and parses field 3 as DDMMYYYY."""
        flipped = change_date_format("EDI_AGG_INPATIENT_060620260000CDT.csv.gz")
        split_name = flipped.split("_")
        assert len(split_name) == 5
        hhmm = ''.join(filter(str.isdigit, split_name[4]))
        assert datetime.datetime.strptime(split_name[3] + hhmm, "%d%m%Y%H%M") == \
            datetime.datetime(2026, 6, 6, 0, 0)

    @pytest.mark.parametrize("name, expected", [
        ("SYNEDI_AGG_INPATIENT_20200611_1451CDT",
         datetime.datetime(2020, 6, 11, 14, 51)),
        ("EDI_AGG_INPATIENT_08272021_0251CDT.csv.gz.filepart",
         datetime.datetime(2021, 8, 27, 2, 51)),
        ("EDI_AGG_INPATIENT_1_05302020_0352CDT.csv.gz",
         datetime.datetime(2020, 5, 30, 3, 52)),
        # no separator between date and time
        ("EDI_AGG_INPATIENT_060620260000CDT.csv.gz",
         datetime.datetime(2026, 6, 6, 0, 0)),
        ("EDI_AGG_INPATIENT_1_060620260000CDT.csv.gz",
         datetime.datetime(2026, 6, 6, 0, 0)),
    ])
    def test_get_timestamp(self, name, expected):
        assert get_timestamp(name) == expected

    @pytest.mark.parametrize("name", [
        "EDI_AGG_OUTPATIENT_20200611_1451CDT.csv.gz",
        "EDI_AGG_INPATIENT_nonsense.csv.gz",
        "EDI_AGG_INPATIENT_123_1451CDT.csv.gz",
    ])
    def test_get_timestamp_unparseable(self, name):
        """Names we can't read sort to the epoch rather than raising."""
        assert get_timestamp(name) == datetime.datetime(1900, 1, 1)
