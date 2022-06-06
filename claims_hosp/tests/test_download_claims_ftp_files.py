# standard
import datetime

# third party
import numpy as np

# first party
from delphi_claims_hosp.download_claims_ftp_files import (flip_YYYYMMDD_to_DDMMYYYY,
                                                          get_timestamp)


class TestDownloadClaimsFtpFiles:
    
    def test_flip_YYYYMMDD_to_DDMMYYYY(self):
        name = "SYNEDI_AGG_INPATIENT_20200611_1451CDT"
        expected = "SYNEDI_AGG_INPATIENT_11062020_1451CDT"
        assert(flip_YYYYMMDD_to_DDMMYYYY(name)==expected)
        
    def test_get_timestamp(self):
        name = "SYNEDI_AGG_INPATIENT_20200611_1451CDT"
        assert(get_timestamp(name).date() == datetime.date(2020, 6, 11))
