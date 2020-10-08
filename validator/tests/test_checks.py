import pytest
from datetime import date, datetime, timedelta
import pandas as pd

from delphi_validator.datafetcher import filename_regex
import delphi_validator.validate
from delphi_validator.validate import Validator


# # Define constants.
# PARAMS = read_params()
# DATA_FILEPATH = PARAMS["input_file"]


class TestDateFilter:

    def test_same_day_filter(self):
        start_date = end_date = datetime.strptime("20200902", "%Y%m%d")
        date_filter = delphi_validator.validate.make_date_filter(
            start_date, end_date)

        filenames = [(f, filename_regex.match(f))
                     for f in ("20200901_county_signal_signal.csv",
                               "20200902_county_signal_signal.csv",
                               "20200903_county_signal_signal.csv")]

        subset_filenames = [(f, m) for (f, m) in filenames if date_filter(m)]

        assert len(subset_filenames) == 1
        assert subset_filenames[0] == "20200902_county_signal_signal.csv"


# class TestValidatorInitialization:

#     def test_empty_df(self):
# 	    validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised) == 0


# class TestCheckMissingDates:

#     def test_empty_df(self):
# 	    validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised) == 0


# class TestCheckSettings:

#     def test_empty_df(self):
# 	    validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised) == 0


# class TestCheckDfFormat:

#     def test_empty_df(self):
# 	    validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised) == 0


# class TestCheckBadGeoId:

#     def test_empty_df(self):
# 	    validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised) == 0


# class TestCheckBadVal:

#     def test_empty_df(self):
# 	    validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised) == 0


# class TestCheckBadSe:

#     def test_empty_df(self):
# 	    validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised) == 0


# class TestCheckBadN:

#     def test_empty_df(self):
# 	    validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised) == 0


# class TestCheckMinDate:

#     def test_empty_df(self):
# 	    validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised) == 0


# class TestCheckMaxDate:

#     def test_empty_df(self):
# 	    validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised) == 0


# class TestCheckMaxReferenceDate:

#     def test_empty_df(self):
# 	    validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised) == 0


# class TestCheckRapidChange:

#     def test_empty_df(self):
# 	    validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised) == 0


# class TestCheckAvgValDiffs:

#     def test_empty_df(self):
# 	    validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised) == 0


# # How?
# class TestValidate:

#     def test_empty_df(self):
# 	    validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised) == 0


# # How?
# class TestExit:

#     def test_empty_df(self):
# 	    validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised) == 0
