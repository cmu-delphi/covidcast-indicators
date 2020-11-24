import sys
from datetime import date, datetime

class ValidationReport:
    def __init__(self, errors_to_suppress):
        self.errors_to_suppress = errors_to_suppress.copy()
        self.num_suppressed = 0
        self.total_checks = 0
        self.raised_errors = []
        self.raised_warnings = []
        self.supressed_errors = []

    def add_raised_error(self, error):
        self.raised_errors.append(error)
        # Convert any dates in check_data_id to strings for the purpose of comparing
        # to manually suppressed errors.
        raised_check_id = tuple([
            item.strftime("%Y-%m-%d") if isinstance(item, (date, datetime))
            else item for item in error.check_data_id])

        if raised_check_id not in self.errors_to_suppress:
            self.supressed_errors.append(error)
        else:
            self.errors_to_suppress.remove(raised_check_id)
            self.num_suppressed += 1


    def increment_total_checks(self):
        self.total_checks += 1

    def add_raised_warning(self, warning):
        self.raised_warnings.append(warning)

    def __str__(self):
        out_str = f"{self.total_checks} checks run\n"
        out_str += f"{len(self.supressed_errors)} checks failed\n"
        out_str += f"{self.num_suppressed} checks suppressed\n"
        out_str += f"{len(self.raised_warnings)} warnings\n"
        for message in self.supressed_errors:
            out_str += f"{message}\n"
        for message in self.raised_warnings:
            out_str += f"{message}\n"
        return out_str

    def print_and_exit(self):
        """
        If any not-suppressed exceptions were raised, print and exit with non-zero status.
        """
        print(self)
        if len(self.supressed_errors) != 0:
            sys.exit(1)
        else:
            sys.exit(0)