# standard
import time

# third party
import pytest

from delphi_utils import get_structured_logger
from delphi_claims_hosp.get_latest_claims_name import get_latest_filename


class TestGetLatestFileName:
    
    start_time = time.time()
    logger = get_structured_logger(
        __name__, filename="./tests/test.log",
        log_exceptions=True)
    
    def test_get_latest_claims_name(self):
        dir_path = "./test_data/"
        
        with pytest.raises(AssertionError):
            get_latest_filename(dir_path, self.logger)
