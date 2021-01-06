"""Validation output reports."""
import sys
from typing import Set, Tuple
from delphi_utils.logger import get_structured_logger

logger = get_structured_logger(__name__)

class ValidationReport:
    """Class for reporting the results of validation."""
    def __init__(self, errors_to_suppress: Set[Tuple[str]]):
        """Initialize a ValidationReport.
        Parameters
        ----------
        errors_to_suppress: Set[Tuple[str]]
            set of (check_name, data_name) tuples to ignore.

        Attributes
        ----------
        errors_to_suppress: Set[Tuple[str]]
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
        self.errors_to_suppress = errors_to_suppress.copy()
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
        """Records a check."""
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
        """String representation of summary of report."""
        out_str = f"{self.total_checks} checks run\n"
        out_str += f"{len(self.unsuppressed_errors)} checks failed\n"
        out_str += f"{self.num_suppressed} checks suppressed\n"
        out_str += f"{len(self.raised_warnings)} warnings\n"
        return out_str

    def log(self):
        """Log errors and warnings."""
        for error in self.unsuppressed_errors:
            logger.critical(str(error))
        for warning in self.raised_warnings:
            logger.warning(str(warning))

    def print_and_exit(self):
        """
        Print results and, if any not-suppressed exceptions were raised, exit with non-zero status.
        """
        print(self.summary)
        self.log()
        if len(self.unsuppressed_errors) == 0:
            sys.exit(0)
        else:
            sys.exit(1)
