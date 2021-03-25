"""Tests for errors.py."""
import datetime as dt
import pytest
from delphi_utils.validator.errors import ValidationFailure

class TestValidationFailure:
    """Tests for ValidationFailure class."""

    def test_init(self):
        """Test initialization of ValidationFailures."""
        # Normal positional arguments.
        vf1 = ValidationFailure("chk", "1990-10-03", "state", "deaths", "msg")
        assert vf1.check_name == "chk"
        assert vf1.date == dt.date(1990, 10, 3)
        assert vf1.geo_type == "state"
        assert vf1.signal == "deaths"
        assert vf1.message == "msg"

        # Missing arguments filled in.
        vf2 = ValidationFailure("chk", dt.date(1990, 10, 3))
        assert vf2.check_name == "chk"
        assert vf2.date == dt.date(1990, 10, 3)
        assert vf2.geo_type is None
        assert vf2.signal is None
        assert vf2.message == ""

        # Values extracted from a filename.
        vf3 = ValidationFailure("chk", filename="20200311_county_cases_7dav.csv",
                                date=dt.date(400, 4, 4), geo_type="ignored", signal="also_ignored")
        assert vf3.check_name == "chk"
        assert vf3.date == dt.date(2020, 3, 11)
        assert vf3.geo_type == "county"
        assert vf3.signal == "cases_7dav"
        assert vf3.message == ""

        # All missing arguments
        vf4 = ValidationFailure()
        assert vf4.check_name is None
        assert vf4.date is None
        assert vf4.geo_type is None
        assert vf4.signal is None
        assert vf4.message == ""

        with pytest.raises(AssertionError,
                           match='`filename` argument expected to be in "{date}_{geo_type}_'\
                                 '{signal}.{extension}" format'):
            # file name not formatted correctly
            ValidationFailure("chk", filename="20200311_county.csv")

        with pytest.raises(ValueError, match='date in `filename` must be in "YYYYMMDD" format'):
            # date in file name not formatted correctly
            ValidationFailure("chk", filename="2020-03-11_county_cases_7dav.csv")

    def test_eq(self):
        """Test equality checking of ValidationFailures."""
        vf = ValidationFailure("a", dt.date(2021, 1, 20), "b", "c", "d")
        assert vf == ValidationFailure("a", dt.date(2021, 1, 20), "b", "c", "")
        assert vf == ValidationFailure(None, dt.date(2021, 1, 20), "b", "c", "")
        assert vf == ValidationFailure("a", None, "b", "c", "")
        assert vf == ValidationFailure("a", dt.date(2021, 1, 20), None, "c", "")
        assert vf == ValidationFailure("a", dt.date(2021, 1, 20), "b", None, "")
        assert vf == ValidationFailure(None, None, "b", "c", "")
        assert vf == ValidationFailure("a", dt.date(2021, 1, 20), None, None, "")
        assert vf == ValidationFailure(None, None, None, None, "")

        assert vf != ValidationFailure("x", dt.date(2021, 1, 20), "b", "c", "")
        assert vf != ValidationFailure(None, dt.date(2021, 1, 21), "b", "c", "")
        assert vf != ValidationFailure("a", None, "x", "c", "")
        assert vf != ValidationFailure("a", dt.date(2021, 1, 20), None, "x", "")
        assert vf != ValidationFailure("a", dt.date(2021, 1, 20), "x", None, "")
        assert vf != ValidationFailure(None, None, "x", "c", "")
        assert vf != ValidationFailure("x", dt.date(2021, 1, 20), None, None, "")

    def test_is_suppressed(self):
        """Tests the suppression of failures."""
        vf = ValidationFailure("a", dt.date(2021, 1, 20), "b", "c", "d")
        assert vf.is_suppressed([ValidationFailure("a", dt.date(2020, 1, 20), "b", "c", ""),
                                 ValidationFailure(None, dt.date(2021, 1, 20), "b", "c", ""),
                                 ValidationFailure("a", None, "b", "c", "")])

        assert not vf.is_suppressed([ValidationFailure("x", dt.date(2021, 1, 20), "b", "c", ""),
                                     ValidationFailure(None, dt.date(2021, 1, 21), "b", "c", ""),
                                     ValidationFailure("a", None, "x", "c", ""),
                                     ValidationFailure("a", dt.date(2021, 1, 20), None, "x", "")])
