"""Validation output reports."""
import sys
from typing import List
from ..logger import get_structured_logger
from .errors import ValidationFailure

class ValidationReport:
    """Class for reporting the results of validation."""

    # pylint: disable=R0902
    def __init__(self, errors_to_suppress: List[ValidationFailure],
                 data_source: str = "", dry_run: bool = False):
        """Initialize a ValidationReport.

        Parameters
        ----------
        errors_to_suppress: List[ValidationFailure]
            List of ValidationFailures to ignore.
        data_source: str
            Name of data source as obtained from params

        Attributes
        ----------
        errors_to_suppress: List[ValidationFailure]
            See above
        data_source: str
            See above
        num_suppressed: int
            Number of errors suppressed
        total_checks: int
            Number of validation checks performed
        raised_errors: List[Exception]
            Errors raised from validation failures
        raised_warnings: List[Exception]
            Warnings raised from validation execution
        unsuppressed_errors: List[Exception]
            Errors raised from validation failures not found in `self.errors_to_suppress`
        elapsed_time_in_seconds: float
            Elapsed time of validation run, rounded down
        """
        self.errors_to_suppress = errors_to_suppress
        self.data_source = data_source
        self.num_suppressed = 0
        self.total_checks = 0
        self.raised_errors = []
        self.raised_warnings = []
        self.unsuppressed_errors = []
        self.dry_run = dry_run
        self.elapsed_time_in_seconds = -1
    # pylint: enable=R0902

    def add_raised_error(self, error):
        """Add an error to the report.

        Parameters
        ----------
        error: Exception
            Error raised in validation

        Returns
        -------
        None
        """
        self.raised_errors.append(error)
        if error.is_suppressed(self.errors_to_suppress):
            self.num_suppressed += 1
        else:
            self.unsuppressed_errors.append(error)

    def increment_total_checks(self):
        """Record a check."""
        self.total_checks += 1

    def set_elapsed_time_in_seconds(self, time):
        """Set elapsed runtime in seconds for later logging."""
        self.elapsed_time_in_seconds = time

    def add_raised_warning(self, warning):
        """Add a warning to the report.

        Parameters
        ----------
        warning: Warning
            Warning raised in validation

        Returns
        -------
        None
        """
        self.raised_warnings.append(warning)

    def log(self, logger=None):
        """Log errors and warnings."""
        if logger is None:
            logger = get_structured_logger(__name__)

        if self.success():
            logger.info("Validation run successful",
                data_source = self.data_source,
                checks_run = self.total_checks,
                checks_failed = len(self.unsuppressed_errors),
                checks_suppressed = self.num_suppressed,
                warnings = len(self.raised_warnings),
                phase = "validation",
                elapsed_time_in_seconds=self.elapsed_time_in_seconds)
        else:
            logger.info("Validation run unsuccessful",
                data_source = self.data_source,
                checks_run = self.total_checks,
                checks_failed = len(self.unsuppressed_errors),
                checks_suppressed = self.num_suppressed,
                warnings = len(self.raised_warnings),
                phase="validation",
                elapsed_time_in_seconds=self.elapsed_time_in_seconds)
        # Threshold for slack alerts if warnings are excessive,
        # Currently extremely strict, set by observation of 1 month's logs
        excessive_warnings = self.total_checks > 0 and \
            (len(self.raised_warnings) > 200 or \
            len(self.raised_warnings) / self.total_checks > 0.015)
        if excessive_warnings:
            logger.info("Excessive number of warnings",
                data_source = self.data_source,
                checks_run = self.total_checks,
                checks_failed = len(self.unsuppressed_errors),
                checks_suppressed = self.num_suppressed,
                warnings = len(self.raised_warnings),
                phase = "validation")
        for error in self.unsuppressed_errors:
            date_str = "*" if error.date is None else error.date.isoformat()
            logger.critical(str(error),
                phase = "validation",
                error_name = error.check_name,
                signal = error.signal,
                resolution = error.geo_type,
                date = date_str)
        for warning in self.raised_warnings:
            date_str = "*" if warning.date is None else warning.date.isoformat()
            logger.warning(str(warning),
                phase="validation",
                error_name = warning.check_name,
                signal = warning.signal,
                resolution = warning.geo_type,
                date = date_str)

    def print_and_exit(self, logger=None, die_on_failures=True):
        """Print results and exit.

        Arguments
        ---------
        die_on_failures: bool
            Whether to return non-zero status if any failures were encountered.
        """
        self.log(logger)
        if self.success():
            sys.exit(0)
        elif die_on_failures:
            sys.exit(1)

    def success(self):
        """Determine if the report corresponds to a successful validation run."""
        return len(self.unsuppressed_errors) == 0 or self.dry_run
