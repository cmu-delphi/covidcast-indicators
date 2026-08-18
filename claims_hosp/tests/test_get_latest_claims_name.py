# standard
import time
from unittest.mock import Mock

# third party
import pytest


from delphi_claims_hosp.get_latest_claims_name import get_latest_filename


class TestGetLatestFileName:
    logger = Mock()
    
    def test_get_latest_claims_name(self):
        dir_path = "./test_data/"
        
        with pytest.raises(AssertionError):
            get_latest_filename(dir_path, self.logger)

    def test_get_latest_claims_name_skips_misnamed(self, tmp_path):
        # a drop misnamed by the old MMDDYYYY flip shouldn't take down the run;
        # reaching the "no drop for today" assert means it was skipped
        (tmp_path / "EDI_AGG_INPATIENT_26200807_1415CDT.csv.gz").touch()

        with pytest.raises(AssertionError):
            get_latest_filename(str(tmp_path), self.logger)
