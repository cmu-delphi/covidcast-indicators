import pytest
from datetime import date, datetime, timedelta
import pandas as pd

from delphi_validator.datafetcher import filename_regex
from delphi_validator.validate import Validator, make_date_filter

import pdb


class TestDateFilter:

    def test_same_day(self):
        start_date = end_date = datetime.strptime("20200902", "%Y%m%d")
        date_filter = make_date_filter(
            start_date, end_date)

        filenames = [(f, filename_regex.match(f))
                     for f in ("20200901_county_signal_signal.csv",
                               "20200902_county_signal_signal.csv",
                               "20200903_county_signal_signal.csv")]

        subset_filenames = [(f, m) for (f, m) in filenames if date_filter(m)]

        assert len(subset_filenames) == 1
        assert subset_filenames[0][0] == "20200902_county_signal_signal.csv"

    def test_inclusive(self):
        start_date = datetime.strptime("20200902", "%Y%m%d")
        end_date = datetime.strptime("20200903", "%Y%m%d")
        date_filter = make_date_filter(
            start_date, end_date)

        filenames = [(f, filename_regex.match(f))
                     for f in ("20200901_county_signal_signal.csv",
                               "20200902_county_signal_signal.csv",
                               "20200903_county_signal_signal.csv",
                               "20200904_county_signal_signal.csv")]

        subset_filenames = [(f, m) for (f, m) in filenames if date_filter(m)]

        assert len(subset_filenames) == 2

    def test_empty(self):
        start_date = datetime.strptime("20200902", "%Y%m%d")
        end_date = datetime.strptime("20200903", "%Y%m%d")
        date_filter = make_date_filter(
            start_date, end_date)

        filenames = [(f, filename_regex.match(f))
                     for f in ()]

        subset_filenames = [(f, m) for (f, m) in filenames if date_filter(m)]

        assert len(subset_filenames) == 0


class TestValidatorInitialization:

    def test_default_settings(self):
        params = {"data_source": "", "start_date": "2020-09-01",
                  "end_date": "2020-09-01"}
        validator = Validator(params)

        assert validator.max_check_lookbehind == timedelta(days=7)
        assert validator.minimum_sample_size == 100
        assert validator.missing_se_allowed == False
        assert validator.missing_sample_size_allowed == False
        assert validator.sanity_check_rows_per_day == True
        assert validator.sanity_check_value_diffs == True
        assert len(validator.suppressed_errors) == 0
        assert isinstance(validator.suppressed_errors, set)
        assert len(validator.raised_errors) == 0


class TestCheckMissingDates:

    def test_empty_filelist(self):
        params = {"data_source": "", "start_date": "2020-09-01",
                  "end_date": "2020-09-09"}
        validator = Validator(params)

        filenames = list()
        validator.check_missing_dates(filenames)

        assert len(validator.raised_errors) == 1
        assert "check_missing_date_files" in [
            err.check_data_id[0] for err in validator.raised_errors]
        assert len(validator.raised_errors[0].expression) == 9

    def test_same_day(self):
        params = {"data_source": "", "start_date": "2020-09-01",
                  "end_date": "2020-09-01"}
        validator = Validator(params)

        filenames = [("20200901_county_signal_signal.csv", "match_obj")]
        validator.check_missing_dates(filenames)

        assert len(validator.raised_errors) == 0
        assert "check_missing_date_files" not in [
            err.check_data_id[0] for err in validator.raised_errors]

    def test_duplicate_dates(self):
        params = {"data_source": "", "start_date": "2020-09-01",
                  "end_date": "2020-09-02"}
        validator = Validator(params)

        filenames = [("20200901_county_signal_signal.csv", "match_obj"),
                     ("20200903_county_signal_signal.csv", "match_obj"),
                     ("20200903_usa_signal_signal.csv", "match_obj"),
                     ("20200903_usa_signal_signal.csv", "match_obj")]
        validator.check_missing_dates(filenames)

        assert len(validator.raised_errors) == 1
        assert "check_missing_date_files" in [
            err.check_data_id[0] for err in validator.raised_errors]
        assert len([err.expression[0] for
                    err in validator.raised_errors if err.check_data_id[0] ==
                    "check_missing_date_files"]) == 1
        assert [err.expression[0] for
                err in validator.raised_errors if err.check_data_id[0] ==
                "check_missing_date_files"][0] == datetime.strptime("20200902", "%Y%m%d").date()


class TestNameFormat:

    def test_match_existence(self):
        pattern_found = filename_regex.match("20200903_usa_signal_signal.csv")
        assert pattern_found

        pattern_found = filename_regex.match("2020090_usa_signal_signal.csv")
        assert not pattern_found

        pattern_found = filename_regex.match("20200903_usa_signal_signal.pdf")
        assert not pattern_found

        pattern_found = filename_regex.match("20200903_usa_.csv")
        assert not pattern_found

    def test_expected_groups(self):
        pattern_found = filename_regex.match(
            "20200903_usa_signal_signal.csv").groupdict()
        assert pattern_found["date"] == "20200903"
        assert pattern_found["geo_type"] == "usa"
        assert pattern_found["signal"] == "signal_signal"


class TestCheckBadGeoId:
    params = {"data_source": "", "start_date": "2020-09-01",
              "end_date": "2020-09-02"}

    def test_empty_df(self):
        validator = Validator(self.params)
        empty_df = pd.DataFrame(columns=["geo_id"])
        validator.check_bad_geo_id(empty_df, "name", "county")

        assert len(validator.raised_errors) == 0

    def test_invalid_geo_type(self):
        validator = Validator(self.params)
        empty_df = pd.DataFrame(columns=["geo_id"])
        validator.check_bad_geo_id(empty_df, "name", "hello")

        assert len(validator.raised_errors) == 1
        assert "check_geo_type" in [
            err.check_data_id[0] for err in validator.raised_errors]
        assert [err.expression for
                err in validator.raised_errors if err.check_data_id[0] ==
                "check_geo_type"][0] == "hello"

    def test_invalid_geo_id_county(self):
        validator = Validator(self.params)
        df = pd.DataFrame(["0", "54321", "123", ".0000",
                           "abc12"], columns=["geo_id"])
        validator.check_bad_geo_id(df, "name", "county")

        assert len(validator.raised_errors) == 1
        assert "check_geo_id_format" in validator.raised_errors[0].check_data_id
        assert len(validator.raised_errors[0].expression) == 4
        assert "54321" not in validator.raised_errors[0].expression

    def test_invalid_geo_id_msa(self):
        validator = Validator(self.params)
        df = pd.DataFrame(["0", "54321", "123", ".0000",
                           "abc12"], columns=["geo_id"])
        validator.check_bad_geo_id(df, "name", "msa")

        assert len(validator.raised_errors) == 1
        assert "check_geo_id_format" in validator.raised_errors[0].check_data_id
        assert len(validator.raised_errors[0].expression) == 4
        assert "54321" not in validator.raised_errors[0].expression

    def test_invalid_geo_id_hrr(self):
        validator = Validator(self.params)
        df = pd.DataFrame(["1", "12", "123", "1234", "12345",
                           "a", ".", "ab1"], columns=["geo_id"])
        validator.check_bad_geo_id(df, "name", "hrr")

        assert len(validator.raised_errors) == 1
        assert "check_geo_id_format" in validator.raised_errors[0].check_data_id
        assert len(validator.raised_errors[0].expression) == 5
        assert "1" not in validator.raised_errors[0].expression
        assert "12" not in validator.raised_errors[0].expression
        assert "123" not in validator.raised_errors[0].expression

    def test_invalid_geo_id_state(self):
        validator = Validator(self.params)
        df = pd.DataFrame(["aa", "hi", "HI", "hawaii",
                           "Hawaii", "a", "H.I."], columns=["geo_id"])
        validator.check_bad_geo_id(df, "name", "state")

        assert len(validator.raised_errors) == 1
        assert "check_geo_id_format" in validator.raised_errors[0].check_data_id
        assert len(validator.raised_errors[0].expression) == 5
        assert "aa" not in validator.raised_errors[0].expression
        assert "hi" not in validator.raised_errors[0].expression

    def test_invalid_geo_id_national(self):
        validator = Validator(self.params)
        df = pd.DataFrame(["usa", "USA", " usa", "us",
                           "usausa", "America"], columns=["geo_id"])
        validator.check_bad_geo_id(df, "name", "national")

        assert len(validator.raised_errors) == 1
        assert "check_geo_id_format" in validator.raised_errors[0].check_data_id
        assert len(validator.raised_errors[0].expression) == 5
        assert "usa" not in validator.raised_errors[0].expression


class TestCheckBadVal:

    def test_empty_df(self):
        validator = Validator()
        empty_df = pd.DataFrame(columns=["val"])
        self.validator.check_bad_val(empty_df, "")

        assert len(self.validator.raised_errors) == 0


# class TestCheckBadSe:

#     def test_empty_df(self):
#         validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised_errors) == 0


# class TestCheckBadN:

#     def test_empty_df(self):
#         validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised_errors) == 0


# class TestCheckMinDate:

#     def test_empty_df(self):
#         validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised_errors) == 0


# class TestCheckMaxDate:

#     def test_empty_df(self):
#         validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised_errors) == 0


# class TestCheckMaxReferenceDate:

#     def test_empty_df(self):
#         validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised_errors) == 0


# class TestCheckRapidChange:

#     def test_empty_df(self):
#         validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised_errors) == 0


# class TestCheckAvgValDiffs:

#     def test_empty_df(self):
#         validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised_errors) == 0


# # How?
# class TestValidate:

#     def test_empty_df(self):
#         validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised_errors) == 0


# # How?
# class TestExit:

#     def test_empty_df(self):
#         validator = Validator()
#         empty_df = pd.DataFrame(columns=["val"])
#         self.validator.check_bad_val(empty_df, "")

#         assert len(self.validator.raised_errors) == 0
