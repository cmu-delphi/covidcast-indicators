# standard
import datetime

# first party
from delphi_doctor_visits.download_claims_ftp_files import (change_date_format,
                                                            get_timestamp)


class TestDownloadClaimsFtpFiles:

    def test_change_date_format(self):
        name = "SYNEDI_AGG_OUTPATIENT_20200611_1451CDT"
        expected = "SYNEDI_AGG_OUTPATIENT_11062020_1451CDT"
        assert(change_date_format(name)==expected)

        # date and time are no longer always separated
        name = "EDI_AGG_OUTPATIENT_060620260000CDT.csv.gz"
        expected = "EDI_AGG_OUTPATIENT_06062026_0000CDT.csv.gz"
        assert(change_date_format(name)==expected)

        name = "EDI_AGG_OUTPATIENT_082720210251CDT.csv.gz"
        expected = "EDI_AGG_OUTPATIENT_27082021_0251CDT.csv.gz"
        assert(change_date_format(name)==expected)

        # chunked drops keep the timestamp in the following field
        name = "EDI_AGG_OUTPATIENT_1_05302020_0352CDT.csv.gz"
        assert(change_date_format(name)==name)

    def test_get_timestamp(self):
        name = "SYNEDI_AGG_OUTPATIENT_20200611_1451CDT"
        assert(get_timestamp(name).date() == datetime.date(2020, 6, 11))

        name = "EDI_AGG_OUTPATIENT_08272021_0251CDT.csv.gz.filepart"
        assert(get_timestamp(name).date() == datetime.date(2021, 8, 27))

        name = "EDI_AGG_OUTPATIENT_1_05302020_0352CDT.csv.gz"
        assert(get_timestamp(name).date() == datetime.date(2020, 5, 30))

        # date and time are no longer always separated
        name = "EDI_AGG_OUTPATIENT_060620260000CDT.csv.gz"
        assert(get_timestamp(name) == datetime.datetime(2026, 6, 6, 0, 0))

        name = "EDI_AGG_OUTPATIENT_1_053020200352CDT.csv.gz"
        assert(get_timestamp(name).date() == datetime.date(2020, 5, 30))

        name = "EDI_AGG_OUTPATIENT_no_date_here.csv.gz"
        assert(get_timestamp(name) == datetime.datetime(1900, 1, 1))
