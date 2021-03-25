"""Tests for delphi_utils.validator.report."""
import mock
from delphi_utils.validator.errors import ValidationFailure
from delphi_utils.validator.report import ValidationReport

class TestValidationReport:
    """Tests for ValidationReport class."""

    ERROR_1 = ValidationFailure("good",
                                filename="20201107_county_sig1.csv",
                                message="msg 1")
    ERROR_2 = ValidationFailure("bad",
                                filename="20201107_county_sig2.csv",
                                message="msg 2")

    def test_add_raised_unsuppressed_error(self):
        """Test that an unsupressed error shows up in the unsuppressed error list."""
        report = ValidationReport([ValidationFailure("good",
                                   filename="20201107_county_sig2.csv",
                                   message="msg 2")])
        report.add_raised_error(self.ERROR_1)
        report.add_raised_error(self.ERROR_2)
        assert report.unsuppressed_errors == [self.ERROR_1, self.ERROR_2]

    def test_add_raised_suppressed_error(self):
        """Test that an supressed error does not show up in the unsuppressed error list."""
        report = ValidationReport([self.ERROR_1])
        report.add_raised_error(self.ERROR_1)

        assert len(report.unsuppressed_errors) == 0
        assert report.num_suppressed == 1

    def test_str(self):
        """Test that the string representation contains all information."""
        report = ValidationReport([self.ERROR_1])
        report.increment_total_checks()
        report.increment_total_checks()
        report.increment_total_checks()
        report.add_raised_warning(ImportWarning("wrong import"))
        report.add_raised_warning(ImportWarning("right import"))
        report.add_raised_error(self.ERROR_1)
        report.add_raised_error(self.ERROR_2)

        assert report.summary() ==\
            "3 checks run\n1 checks failed\n1 checks suppressed\n2 warnings\n"

    @mock.patch("delphi_utils.validator.report.logger")
    def test_log(self, mock_logger):
        """Test that the logs contain all failures and warnings."""
        report = ValidationReport([self.ERROR_1])
        report.increment_total_checks()
        report.increment_total_checks()
        report.increment_total_checks()
        report.add_raised_warning(ImportWarning("wrong import"))
        report.add_raised_warning(ImportWarning("right import"))
        report.add_raised_error(self.ERROR_1)
        report.add_raised_error(self.ERROR_2)

        report.log()
        mock_logger.critical.assert_called_once_with(
            "bad failed for sig2 at resolution county on 2020-11-07: msg 2")
        mock_logger.warning.assert_has_calls([mock.call("wrong import"), mock.call("right import")])
