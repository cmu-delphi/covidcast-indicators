# standard
import time
from unittest.mock import Mock

# third party
import pytest


from delphi_doctor_visits.get_latest_claims_name import get_latest_filename


class TestGetLatestFileName:
    logger = Mock()
    dir_path = "test_data"

    def test_get_latest_claims_name(self):
        with pytest.raises(AssertionError):
            get_latest_filename(self.dir_path, self.logger)

    def test_get_latest_claims_name_with_issue_date(self):
        result = get_latest_filename(self.dir_path, self.logger, issue_date="2020-02-07")
        assert str(result) == f"{self.dir_path}/SYNEDI_AGG_OUTPATIENT_07022020_1455CDT.csv"
