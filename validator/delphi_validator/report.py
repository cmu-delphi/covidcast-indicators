"""Validation output reports."""
import sys
from datetime import date, datetime
from typing import List, Tuple

class ValidationReport:
    """Class for reporting the results of validation."""
    def __init__(self, errors_to_suppress: List[Tuple[str]]):
        """Initialize a ValidationReport.
        Parameters
        ----------
        errors_to_suppress: List[Tuple[str]]
            List of error identifications to ignore.

        Attributes
        ----------
        errors_to_suppress: List[Tuple[str]]
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
        # Convert any dates in check_data_id to strings for the purpose of comparing
        # to manually suppressed errors.
        raised_check_id = tuple([
            item.strftime("%Y-%m-%d") if isinstance(item, (date, datetime))
            else item for item in error.check_data_id])

        if raised_check_id in self.errors_to_suppress:
            self.errors_to_suppress.remove(raised_check_id)
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
        error: Exception
            Error raised in validation

        Returns
        -------
        None
        """
        self.raised_warnings.append(warning)

    def __str__(self):
        """String representation of report."""
        out_str = f"{self.total_checks} checks run\n"
        out_str += f"{len(self.unsuppressed_errors)} checks failed\n"
        out_str += f"{self.num_suppressed} checks suppressed\n"
        out_str += f"{len(self.raised_warnings)} warnings\n"
        for message in self.unsuppressed_errors:
            out_str += f"{message}\n"
        for message in self.raised_warnings:
            out_str += f"{message}\n"
        return out_str

    def print_and_exit(self):
        """
        Print results and, if any not-suppressed exceptions were raised, exit with non-zero status.
        """
        print(self)
        if len(self.unsuppressed_errors) != 0:
            sys.exit(1)
        else:
            sys.exit(0)
