"""Tests for errors.py."""
import datetime as dt
from delphi_validator.errors import ValidationFailure

class TestValidationFailure:
    """Tests for ValidationFailure class."""

    def test_is_suppressed(self):
        """Tests the suppression of failures."""
        vf = ValidationFailure("a", dt.date(2021, 1, 20), "b", "c", "d")
        assert vf.is_suppressed([ValidationFailure("a", dt.date(2021, 1, 20), "b", "c", "")])
        assert vf.is_suppressed([ValidationFailure(None, dt.date(2021, 1, 20), "b", "c", "")])
        assert vf.is_suppressed([ValidationFailure("a", None, "b", "c", "")])
        assert vf.is_suppressed([ValidationFailure("a", dt.date(2021, 1, 20), None, "c", "")])
        assert vf.is_suppressed([ValidationFailure("a", dt.date(2021, 1, 20), "b", None, "")])
        assert vf.is_suppressed([ValidationFailure(None, None, "b", "c", "")])
        assert vf.is_suppressed([ValidationFailure("a", dt.date(2021, 1, 20), None, None, "")])
        assert vf.is_suppressed([ValidationFailure(None, None, None, None, "")])

    def test_not_is_suppressed(self):
        """Tests the non-suppression of failures."""
        vf = ValidationFailure("a", dt.date(2021, 1, 20), "b", "c", "d")
        assert not vf.is_suppressed([ValidationFailure("x", dt.date(2021, 1, 20), "b", "c", "")])
        assert not vf.is_suppressed([ValidationFailure(None, dt.date(2021, 1, 21), "b", "c", "")])
        assert not vf.is_suppressed([ValidationFailure("a", None, "x", "c", "")])
        assert not vf.is_suppressed([ValidationFailure("a", dt.date(2021, 1, 20), None, "x", "")])
        assert not vf.is_suppressed([ValidationFailure("a", dt.date(2021, 1, 20), "x", None, "")])
        assert not vf.is_suppressed([ValidationFailure(None, None, "x", "c", "")])
        assert not vf.is_suppressed([ValidationFailure("x", dt.date(2021, 1, 20), None, None, "")])
