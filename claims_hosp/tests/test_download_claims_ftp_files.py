# standard
import datetime
import re

# third party
import numpy as np

# first party
from delphi_claims_hosp.download_claims_ftp_files import (change_date_format,
                                                          get_timestamp)

OLD_FILENAME_TIMESTAMP = re.compile(
    r".*EDI_AGG_INPATIENT_[0-9]_(?P<ymd>[0-9]*)_(?P<hm>[0-9]*)[^0-9]*")
NEW_FILENAME_TIMESTAMP = re.compile(r".*EDI_AGG_INPATIENT_(?P<ymd>[0-9]*)_(?P<hm>[0-9]*)[^0-9]*")

class TestDownloadClaimsFtpFiles:
    
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
