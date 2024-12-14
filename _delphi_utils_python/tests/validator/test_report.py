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
    WARNING_1 = ValidationFailure("wrong import", date = None)
    WARNING_2 = ValidationFailure("right import", date = None)

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
        report.add_raised_warning(self.WARNING_1)
        report.add_raised_warning(self.WARNING_2)
        report.add_raised_error(self.ERROR_1)
        report.add_raised_error(self.ERROR_2)

    def test_log(self):
        """Test that the logs contain all failures and warnings."""
        mock_logger = mock.Mock()
        report = ValidationReport([self.ERROR_1])
        report.increment_total_checks()
        report.increment_total_checks()
        report.increment_total_checks()
        report.add_raised_warning(self.WARNING_1)
        report.add_raised_warning(self.WARNING_2)
        report.add_raised_error(self.ERROR_1)
        report.add_raised_error(self.ERROR_2)

        report.log(mock_logger)
        mock_logger.critical.assert_called_once_with(
            "bad failed for sig2 at resolution county on 2020-11-07: msg 2", 
            phase = "validation", error_name = "bad",
            signal = "sig2", resolution = "county",
            date = "2020-11-07")
        mock_logger.warning.assert_has_calls(
            [mock.call("wrong import failed for None at resolution None on *: ",
                phase = "validation",
                error_name = "wrong import",
                signal = None,
                resolution = None,
                date = '*'), 
            mock.call("right import failed for None at resolution None on *: ",
                phase = "validation",
                error_name = "right import",
                signal = None,
                resolution = None,
                date = '*')])
