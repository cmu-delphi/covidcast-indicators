# standard
from unittest.mock import Mock

# third party

# first party
from delphi_claims_hosp.modify_claims_drops import (modify_and_write)


class TestDropsModification:
    
    def test_modify_and_write(self):
        data_path = "./test_data/SYNEDI_AGG_INPATIENT_11062020_1451CDT.csv.gz"
        logger = Mock()
        modify_and_write(data_path, logger, force=False)
