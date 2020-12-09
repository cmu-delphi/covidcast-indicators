"""Tests for delphi_validator.report."""
from datetime import date
from delphi_validator.errors import ValidationError
from delphi_validator.report import ValidationReport

class TestValidationReport:
    """Tests for ValidationReport class."""

    ERROR_1 = ValidationError(("good", date(2020, 10, 5)), "exp 1", "msg 1")
    ERROR_2 = ValidationError(("bad", date(2020, 11, 18)), "exp 2", "msg 2")

    def test_add_raised_unsuppressed_error(self):
        """Test that an unsupressed error shows up in the unsuppressed error list."""
        report = ValidationReport([("bad", "2020-10-05")])
        report.add_raised_error(self.ERROR_1)
        report.add_raised_error(self.ERROR_2)
        assert report.unsuppressed_errors == [self.ERROR_1, self.ERROR_2]

    def test_add_raised_suppressed_error(self):
        """Test that an supressed error does not show up in the unsuppressed error list."""
        report = ValidationReport([("good", "2020-10-05")])
        report.add_raised_error(self.ERROR_1)

        assert len(report.unsuppressed_errors) == 0
        assert report.num_suppressed == 1
        assert len(report.errors_to_suppress) == 0

        # Each error can only be surpressed once.
        report.add_raised_error(self.ERROR_1)
        assert report.unsuppressed_errors == [self.ERROR_1]

    def test_str(self):
        """Test that the string representation contains all information."""
        report = ValidationReport([("good", "2020-10-05")])
        report.increment_total_checks()
        report.increment_total_checks()
        report.increment_total_checks()
        report.add_raised_warning(ImportWarning("wrong import"))
        report.add_raised_warning(ImportWarning("right import"))
        report.add_raised_error(self.ERROR_1)
        report.add_raised_error(self.ERROR_2)

        assert str(report) == "3 checks run\n1 checks failed\n1 checks suppressed\n2 warnings\n"\
            "(('bad', datetime.date(2020, 11, 18)), 'exp 2', 'msg 2')\nwrong import\nright import\n"
