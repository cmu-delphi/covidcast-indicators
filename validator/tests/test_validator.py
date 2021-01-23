"""Tests for Validator"""
import pytest
from delphi_validator.errors import ValidationFailure
from delphi_validator.validate import Validator

class TestValidatorInitialization:
    """Tests for proper initialization."""
    def test_default_settings(self):
        """Test default initialization."""
        params = {
            "global": {
                "data_source": "",
                "span_length": 0,
                "end_date": "2020-09-01"
            }
        }
        validator = Validator(params)
        assert len(validator.suppressed_errors) == 0
        assert isinstance(validator.suppressed_errors, list)

    def test_suppressed_errors(self):
        """Test initialization with suppressed errors."""
        params = {
            "global": {
                "data_source": "",
                "span_length": 0,
                "end_date": "2020-09-01",
                "suppressed_errors": [["a", None, None, "b"],
                                      ["c", None, None, "d"]]
            }
        }

        validator = Validator(params)
        assert validator.suppressed_errors == [ValidationFailure("a", None, None, "b", ""),
                                               ValidationFailure("c", None, None, "d", "")]

    def test_incorrect_suppressed_errors(self):
        """Test initialization with improperly coded suppressed errors."""
        with pytest.raises(AssertionError):
            # entry of length not equal to 2
            Validator({
                "global": {
                    "data_source": "",
                    "span_length": 0,
                    "end_date": "2020-09-01",
                    "suppressed_errors": [["a", None, None, "b"],
                                          ["c", None, None, "d"],
                                          ["ab"]]
                }
            })

        with pytest.raises(AssertionError):
            # entry that is not a list
            Validator({
                "global": {
                    "data_source": "",
                    "span_length": 0,
                    "end_date": "2020-09-01",
                    "suppressed_errors": [["a", None, None, "b"],
                                          ["c", None, None, "d"],
                                          "ab"]
                }
            })
