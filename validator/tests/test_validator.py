""""""
from datetime import date, datetime

from delphi_validator.datafetcher import FILENAME_REGEX, make_date_filter
from delphi_validator.validate import Validator

class TestValidatorInitialization:

    def test_default_settings(self):
        params = {"data_source": "", "span_length": 0,
                  "end_date": "2020-09-01", "expected_lag": {}}
        validator = Validator(params)
        assert len(validator.suppressed_errors) == 0
        assert isinstance(validator.suppressed_errors, set)
