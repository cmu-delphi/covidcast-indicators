"""Validation output reports."""
import sys
from typing import List
from ..logger import get_structured_logger
from .errors import ValidationFailure

class ValidationReport:
    """Class for reporting the results of validation."""

    def __init__(self, errors_to_suppress: List[ValidationFailure]):
        """Initialize a ValidationReport.

        Parameters
        ----------
        errors_to_suppress: List[ValidationFailure]
            List of ValidationFailures to ignore.

        Attributes
        ----------
        errors_to_suppress: List[ValidationFailure]
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
        """
        self.errors_to_suppress = errors_to_suppress
        self.num_suppressed = 0
        self.total_checks = 0
        self.raised_errors = []
        self.raised_warnings = []
        self.unsuppressed_errors = []

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

    def summary(self):
        """Represent summary of report as a string."""
        out_str = f"{self.total_checks} checks run\n"
        out_str += f"{len(self.unsuppressed_errors)} checks failed\n"
        out_str += f"{self.num_suppressed} checks suppressed\n"
        out_str += f"{len(self.raised_warnings)} warnings\n"
        return out_str

    def log(self, logger=None):
        """Log errors and warnings."""
        if logger is None:
            logger = get_structured_logger(__name__)

        for error in self.unsuppressed_errors:
            logger.critical(str(error))
        for warning in self.raised_warnings:
            logger.warning(str(warning))

    def print_and_exit(self, logger=None, die_on_failures=True):
        """Print results and exit.

        Arguments
        ---------
        die_on_failures: bool
            Whether to return non-zero status if any failures were encountered.
        """
        print(self.summary())
        self.log(logger)
        if self.success():
            sys.exit(0)
        elif die_on_failures:
            sys.exit(1)

    def success(self):
        """Determine if the report corresponds to a successful validation run."""
        return len(self.unsuppressed_errors) == 0
