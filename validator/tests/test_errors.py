"""Tests for errors.py."""
from delphi_validator.errors import ValidationFailure

class TestValidationFailure:
    """Tests for ValidationFailure class."""

    def test_is_suppressed(self):
        """Tests the suppression of failures."""
        vf = ValidationFailure("a", "b", "c")
        assert vf.is_suppressed(set([("a", "b")]))
        assert vf.is_suppressed(set([("*", "b")]))
        assert vf.is_suppressed(set([("a", "*")]))
        assert not vf.is_suppressed(set([("c", "*")]))
        assert not vf.is_suppressed(set([("*", "*")]))
        assert not vf.is_suppressed(set([("c", "d")]))
