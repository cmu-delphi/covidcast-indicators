import pytest
import pandas as pd

from delphi_validator.validate import Validator

# # Define constants.
# PARAMS = read_params()
# DATA_FILEPATH = PARAMS["input_file"]


class TestCheckBadVal:
    validator = Validator()

    def test_empty_df(self):
        empty_df = pd.DataFrame(columns=["val"])
        self.validator.check_bad_val(empty_df, "")

        assert len(self.validator.raised) == 0
