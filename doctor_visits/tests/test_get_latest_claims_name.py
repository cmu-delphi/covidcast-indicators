# standard
import time
from unittest.mock import Mock

# third party
import pytest


from delphi_doctor_visits.get_latest_claims_name import get_latest_filename


class TestGetLatestFileName:
    logger = Mock()
    
    def test_get_latest_claims_name(self):
        dir_path = "./test_data/"
        
        with pytest.raises(AssertionError):
            get_latest_filename(dir_path, self.logger)
