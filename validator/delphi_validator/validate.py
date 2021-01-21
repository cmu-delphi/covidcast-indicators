# -*- coding: utf-8 -*-
"""
Tools to validate CSV source data, including various check methods.
"""
from .datafetcher import load_all_files
from .dynamic import DynamicValidator
from .report import ValidationReport
from .static import StaticValidator
from .utils import aggregate_frames, TimeWindow

class Validator():
    """ Class containing validation() function and supporting functions. Stores a list
    of all raised errors, and user settings. """

    def __init__(self, params):
        """
        Initialize object and set parameters.

        Arguments:
            - params: dictionary of user settings; if empty, defaults will be used
        """
        suppressed_errors =  params["global"].get('suppressed_errors', [])
        for entry in suppressed_errors:
            assert isinstance(entry, list)
            assert len(entry) == 2

        self.suppressed_errors = {tuple(item) for item in suppressed_errors}

        # Date/time settings
        self.time_window = TimeWindow.from_params(params["global"]["end_date"],
                                                  params["global"]["span_length"])

        self.static_validation = StaticValidator(params)
        self.dynamic_validation = DynamicValidator(params)

    def validate(self, export_dir):
        """
        Runs all data checks.

        Arguments:
            - export_dir: path to data CSVs

        Returns:
            - ValidationReport collating the validation outcomes
        """
        report = ValidationReport(self.suppressed_errors)
        frames_list = load_all_files(export_dir, self.time_window.start_date,
                                     self.time_window.end_date)
        self.static_validation.validate(frames_list, report)
        all_frames = aggregate_frames(frames_list)
        self.dynamic_validation.validate(all_frames, report)
        return report
