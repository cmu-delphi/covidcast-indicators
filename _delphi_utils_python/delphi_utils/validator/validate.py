# -*- coding: utf-8 -*-
"""Tools to validate CSV source data, including various check methods."""
from .datafetcher import load_all_files
from .dynamic import DynamicValidator
from .errors import ValidationFailure
from .report import ValidationReport
from .static import StaticValidator
from .utils import aggregate_frames, TimeWindow, end_date_helper

class Validator:
    """Class containing validation() function and supporting functions.

    Stores a list of all raised errors, and user settings.
    """

    def __init__(self, params):
        """
        Initialize object and set parameters.

        Arguments:
            - params: dictionary of user settings read from params.json file; if empty, defaults
                will be used
        """
        self.export_dir = params["common"]["export_dir"]

        assert "validation" in params, "params must have a top-level 'validation' object to run "\
            "validation"
        validation_params = params["validation"]
        suppressed_errors =  validation_params["common"].get('suppressed_errors', [])
        for entry in suppressed_errors:
            assert isinstance(entry, dict), "suppressed_errors must be a list of objects"
            assert set(entry.keys()).issubset(["check_name", "date", "geo_type", "signal"]),\
                'suppressed_errors may only have fields "check_name", "date", "geo_type", "signal"'

        self.suppressed_errors = [ValidationFailure(**entry) for entry in suppressed_errors]

        # Date/time settings
        validation_params["common"]["end_date"] = end_date_helper(validation_params)
        self.time_window = TimeWindow.from_params(validation_params["common"]["end_date"],
                                                  validation_params["common"]["span_length"])
        self.data_source = validation_params["common"].get("data_source", "")
        self.dry_run = validation_params["common"].get("dry_run", False)

        self.static_validation = StaticValidator(validation_params)
        self.dynamic_validation = DynamicValidator(validation_params)

    def validate(self):
        """
        Run all data checks.

        Arguments:
            - export_dir: path to data CSVs

        Returns:
            - ValidationReport collating the validation outcomes
        """
        report = ValidationReport(self.suppressed_errors, self.data_source, self.dry_run)
        frames_list = load_all_files(self.export_dir, self.time_window.start_date,
                                     self.time_window.end_date)
        self.static_validation.validate(frames_list, report)
        all_frames = aggregate_frames(frames_list)
        self.dynamic_validation.validate(all_frames, report)
        return report
