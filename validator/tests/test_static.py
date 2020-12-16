""""""
from datetime import datetime
import numpy as np
import pandas as pd

from delphi_validator.datafetcher import FILENAME_REGEX
from delphi_validator.report import ValidationReport
from delphi_validator.static import StaticValidation

class TestCheckMissingDates:

    def test_empty_filelist(self):
        params = {"data_source": "", "span_length": 8,
                  "end_date": "2020-09-09", "expected_lag": {}}
        validator = StaticValidation(params)
        report = ValidationReport(validator.suppressed_errors)
        report = ValidationReport(validator.suppressed_errors)

        filenames = list()
        validator.check_missing_date_files(filenames, report)

        assert len(report.raised_errors) == 1
        assert "check_missing_date_files" in [
            err.check_data_id[0] for err in report.raised_errors]
        assert len(report.raised_errors[0].expression) == 9

    def test_same_day(self):
        params = {"data_source": "", "span_length": 0,
                  "end_date": "2020-09-01", "expected_lag": {}}
        validator = StaticValidation(params)
        report = ValidationReport(validator.suppressed_errors)

        filenames = [("20200901_county_signal_signal.csv", "match_obj")]
        validator.check_missing_date_files(filenames, report)

        assert len(report.raised_errors) == 0
        assert "check_missing_date_files" not in [
            err.check_data_id[0] for err in report.raised_errors]

    def test_duplicate_dates(self):
        params = {"data_source": "", "span_length": 1,
                  "end_date": "2020-09-02", "expected_lag": {}}
        validator = StaticValidation(params)
        report = ValidationReport(validator.suppressed_errors)

        filenames = [("20200901_county_signal_signal.csv", "match_obj"),
                     ("20200903_county_signal_signal.csv", "match_obj"),
                     ("20200903_usa_signal_signal.csv", "match_obj"),
                     ("20200903_usa_signal_signal.csv", "match_obj")]
        validator.check_missing_date_files(filenames, report)

        assert len(report.raised_errors) == 1
        assert "check_missing_date_files" in [
            err.check_data_id[0] for err in report.raised_errors]
        assert len([err.expression[0] for
                    err in report.raised_errors if err.check_data_id[0] ==
                    "check_missing_date_files"]) == 1
        assert [err.expression[0] for
                err in report.raised_errors if err.check_data_id[0] ==
                "check_missing_date_files"][0] == datetime.strptime("20200902", "%Y%m%d").date()


class TestNameFormat:

    def test_match_existence(self):
        pattern_found = FILENAME_REGEX.match("20200903_usa_signal_signal.csv")
        assert pattern_found

        pattern_found = FILENAME_REGEX.match("2020090_usa_signal_signal.csv")
        assert not pattern_found

        pattern_found = FILENAME_REGEX.match("20200903_usa_signal_signal.pdf")
        assert not pattern_found

        pattern_found = FILENAME_REGEX.match("20200903_usa_.csv")
        assert not pattern_found

    def test_expected_groups(self):
        pattern_found = FILENAME_REGEX.match(
            "20200903_usa_signal_signal.csv").groupdict()
        assert pattern_found["date"] == "20200903"
        assert pattern_found["geo_type"] == "usa"
        assert pattern_found["signal"] == "signal_signal"


class TestCheckBadGeoIdFormat:
    params = {"data_source": "", "span_length": 0,
              "end_date": "2020-09-02", "expected_lag": {}}

    def test_empty_df(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        empty_df = pd.DataFrame(columns=["geo_id"], dtype=str)
        validator.check_bad_geo_id_format(empty_df, "name", "county", report)

        assert len(report.raised_errors) == 0

    def test_invalid_geo_type(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        empty_df = pd.DataFrame(columns=["geo_id"], dtype=str)
        validator.check_bad_geo_id_format(empty_df, "name", "hello", report)

        assert len(report.raised_errors) == 1
        assert "check_geo_type" in [
            err.check_data_id[0] for err in report.raised_errors]
        assert [err.expression for
                err in report.raised_errors if err.check_data_id[0] ==
                "check_geo_type"][0] == "hello"

    def test_invalid_geo_id_county(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        df = pd.DataFrame(["0", "54321", "123", ".0000",
                           "abc12"], columns=["geo_id"])
        validator.check_bad_geo_id_format(df, "name", "county", report)

        assert len(report.raised_errors) == 1
        assert "check_geo_id_format" in report.raised_errors[0].check_data_id
        assert len(report.raised_errors[0].expression) == 2
        assert "54321" not in report.raised_errors[0].expression

    def test_invalid_geo_id_msa(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        df = pd.DataFrame(["0", "54321", "123", ".0000",
                           "abc12"], columns=["geo_id"])
        validator.check_bad_geo_id_format(df, "name", "msa", report)

        assert len(report.raised_errors) == 1
        assert "check_geo_id_format" in report.raised_errors[0].check_data_id
        assert len(report.raised_errors[0].expression) == 2
        assert "54321" not in report.raised_errors[0].expression

    def test_invalid_geo_id_hrr(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        df = pd.DataFrame(["1", "12", "123", "1234", "12345",
                           "a", ".", "ab1"], columns=["geo_id"])
        validator.check_bad_geo_id_format(df, "name", "hrr", report)

        assert len(report.raised_errors) == 1
        assert "check_geo_id_format" in report.raised_errors[0].check_data_id
        assert len(report.raised_errors[0].expression) == 5
        assert "1" not in report.raised_errors[0].expression
        assert "12" not in report.raised_errors[0].expression
        assert "123" not in report.raised_errors[0].expression

    def test_invalid_geo_id_state(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        df = pd.DataFrame(["aa", "hi", "HI", "hawaii",
                           "Hawaii", "a", "H.I."], columns=["geo_id"])
        validator.check_bad_geo_id_format(df, "name", "state", report)

        assert len(report.raised_errors) == 1
        assert "check_geo_id_format" in report.raised_errors[0].check_data_id
        assert len(report.raised_errors[0].expression) == 4
        assert "aa" not in report.raised_errors[0].expression
        assert "hi" not in report.raised_errors[0].expression
        assert "HI" not in report.raised_errors[0].expression

    def test_invalid_geo_id_national(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        df = pd.DataFrame(["usa", "SP", " us", "us",
                           "usausa", "US"], columns=["geo_id"])
        validator.check_bad_geo_id_format(df, "name", "national", report)

        assert len(report.raised_errors) == 1
        assert "check_geo_id_format" in report.raised_errors[0].check_data_id
        assert len(report.raised_errors[0].expression) == 3
        assert "us" not in report.raised_errors[0].expression
        assert "US" not in report.raised_errors[0].expression
        assert "SP" not in report.raised_errors[0].expression

class TestDuplicatedRows:
    params = {"data_source": "", "span_length": 1,
              "end_date": "2020-09-02", "expected_lag": {}}
    def test_no_duplicates(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        df = pd.DataFrame([["a", "1"], ["b", "2"], ["c", "3"]])
        validator.check_duplicate_rows(df, "file", report)
        assert len(report.raised_warnings) == 0

    def test_single_column_duplicates_but_not_row(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        df = pd.DataFrame([["a", "1"], ["a", "2"], ["b", "2"]])
        validator.check_duplicate_rows(df, "file", report)
        assert len(report.raised_warnings) == 0

    def test_non_consecutive_duplicates(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        df = pd.DataFrame([["a", "1"], ["b", "2"], ["a", "1"]])
        validator.check_duplicate_rows(df, "file", report)
        assert len(report.raised_warnings) == 1
        assert report.raised_warnings[0].expression == [2]
        assert report.raised_warnings[0].check_data_id[1] == "file"

    def test_multiple_distinct_duplicates(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        df = pd.DataFrame([["a", "1"], ["b", "2"], ["a", "1"], ["b", "2"]])
        validator.check_duplicate_rows(df, "file", report)
        assert len(report.raised_warnings) == 1
        assert report.raised_warnings[0].expression == [2, 3]

    def test_more_than_two_copies(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        df = pd.DataFrame([["a", "1"], ["b", "2"], ["b", "2"], ["b", "2"]])
        validator.check_duplicate_rows(df, "file", report)
        assert len(report.raised_warnings) == 1
        assert report.raised_warnings[0].expression == [2, 3]

class TestCheckBadGeoIdValue:
    params = {"data_source": "", "span_length": 0,
              "end_date": "2020-09-02", "expected_lag": {},
              "validator_static_file_dir": "../static"}

    def test_empty_df(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        empty_df = pd.DataFrame(columns=["geo_id"], dtype=str)
        validator.check_bad_geo_id_value(empty_df, "name", "county", report)
        assert len(report.raised_errors) == 0

    def test_invalid_geo_id_county(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        df = pd.DataFrame(["01001", "88888", "99999"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, "name", "county", report)

        assert len(report.raised_errors) == 1
        assert "check_bad_geo_id_value" in report.raised_errors[0].check_data_id
        assert len(report.raised_errors[0].expression) == 2
        assert "01001" not in report.raised_errors[0].expression
        assert "88888" in report.raised_errors[0].expression
        assert "99999" in report.raised_errors[0].expression

    def test_invalid_geo_id_msa(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        df = pd.DataFrame(["10180", "88888", "99999"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, "name", "msa", report)

        assert len(report.raised_errors) == 1
        assert "check_bad_geo_id_value" in report.raised_errors[0].check_data_id
        assert len(report.raised_errors[0].expression) == 2
        assert "10180" not in report.raised_errors[0].expression
        assert "88888" in report.raised_errors[0].expression
        assert "99999" in report.raised_errors[0].expression

    def test_invalid_geo_id_hrr(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        df = pd.DataFrame(["1", "11", "111", "8", "88",
                           "888"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, "name", "hrr", report)

        assert len(report.raised_errors) == 1
        assert "check_bad_geo_id_value" in report.raised_errors[0].check_data_id
        assert len(report.raised_errors[0].expression) == 3
        assert "1" not in report.raised_errors[0].expression
        assert "11" not in report.raised_errors[0].expression
        assert "111" not in report.raised_errors[0].expression
        assert "8" in report.raised_errors[0].expression
        assert "88" in report.raised_errors[0].expression
        assert "888" in report.raised_errors[0].expression

    def test_invalid_geo_id_state(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        df = pd.DataFrame(["aa", "ak"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, "name", "state", report)

        assert len(report.raised_errors) == 1
        assert "check_bad_geo_id_value" in report.raised_errors[0].check_data_id
        assert len(report.raised_errors[0].expression) == 1
        assert "ak" not in report.raised_errors[0].expression
        assert "aa" in report.raised_errors[0].expression

    def test_uppercase_geo_id(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        df = pd.DataFrame(["ak", "AK"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, "name", "state", report)

        assert len(report.raised_errors) == 0
        assert len(report.raised_warnings) == 1
        assert "check_geo_id_lowercase" in report.raised_warnings[0].check_data_id
        assert "AK" in report.raised_warnings[0].expression

    def test_invalid_geo_id_national(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        df = pd.DataFrame(["us", "zz"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, "name", "national", report)

        assert len(report.raised_errors) == 1
        assert "check_bad_geo_id_value" in report.raised_errors[0].check_data_id
        assert len(report.raised_errors[0].expression) == 1
        assert "us" not in report.raised_errors[0].expression
        assert "zz" in report.raised_errors[0].expression


class TestCheckBadVal:
    params = {"data_source": "", "span_length": 1,
              "end_date": "2020-09-02", "expected_lag": {}}

    def test_empty_df(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        empty_df = pd.DataFrame(columns=["val"])
        validator.check_bad_val(empty_df, "", "", report)
        validator.check_bad_val(empty_df, "", "prop", report)
        validator.check_bad_val(empty_df, "", "pct", report)

        assert len(report.raised_errors) == 0

    def test_missing(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        df = pd.DataFrame([np.nan], columns=["val"])
        validator.check_bad_val(df, "name", "signal", report)

        assert len(report.raised_errors) == 1
        assert "check_val_missing" in report.raised_errors[0].check_data_id

    def test_lt_0(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        df = pd.DataFrame([-5], columns=["val"])
        validator.check_bad_val(df, "name", "signal", report)

        assert len(report.raised_errors) == 1
        assert "check_val_lt_0" in report.raised_errors[0].check_data_id

    def test_gt_max_pct(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        df = pd.DataFrame([1e7], columns=["val"])
        validator.check_bad_val(df, "name", "pct", report)

        assert len(report.raised_errors) == 1
        assert "check_val_pct_gt_100" in report.raised_errors[0].check_data_id

    def test_gt_max_prop(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        df = pd.DataFrame([1e7], columns=["val"])
        validator.check_bad_val(df, "name", "prop", report)

        assert len(report.raised_errors) == 1
        assert "check_val_prop_gt_100k" in report.raised_errors[0].check_data_id


class TestCheckBadSe:
    params = {"data_source": "", "span_length": 1,
              "end_date": "2020-09-02", "expected_lag": {}}

    def test_empty_df(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        empty_df = pd.DataFrame(
            columns=["val", "se", "sample_size"], dtype=float)
        validator.check_bad_se(empty_df, "", report)

        assert len(report.raised_errors) == 0

        validator.missing_se_allowed = True
        validator.check_bad_se(empty_df, "", report)

        assert len(report.raised_errors) == 0

    def test_missing(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        validator.missing_se_allowed = True
        df = pd.DataFrame([[np.nan, np.nan, np.nan]], columns=[
                          "val", "se", "sample_size"])
        validator.check_bad_se(df, "name", report)

        assert len(report.raised_errors) == 0

        validator.missing_se_allowed = False
        validator.check_bad_se(df, "name", report)

        assert len(report.raised_errors) == 2
        assert "check_se_not_missing_and_in_range" in [
            err.check_data_id[0] for err in report.raised_errors]
        assert "check_se_many_missing" in [
            err.check_data_id[0] for err in report.raised_errors]

    def test_e_0_missing_allowed(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        validator.missing_se_allowed = True
        df = pd.DataFrame([[1, 0, 200], [1, np.nan, np.nan], [
                          1, np.nan, np.nan]], columns=["val", "se", "sample_size"])
        validator.check_bad_se(df, "name", report)

        assert len(report.raised_errors) == 2
        assert "check_se_missing_or_in_range" in [
            err.check_data_id[0] for err in report.raised_errors]
        assert "check_se_0" in [
            err.check_data_id[0] for err in report.raised_errors]

    def test_e_0_missing_not_allowed(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        validator.missing_se_allowed = False
        df = pd.DataFrame([[1, 0, 200], [1, 0, np.nan], [
                          1, np.nan, np.nan]], columns=["val", "se", "sample_size"])
        validator.check_bad_se(df, "name", report)

        assert len(report.raised_errors) == 2
        assert "check_se_not_missing_and_in_range" in [
            err.check_data_id[0] for err in report.raised_errors]
        assert "check_se_0" in [
            err.check_data_id[0] for err in report.raised_errors]

    def test_jeffreys(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        validator.missing_se_allowed = False
        df = pd.DataFrame([[0, 0, 200], [1, 0, np.nan], [
                          1, np.nan, np.nan]], columns=["val", "se", "sample_size"])
        validator.check_bad_se(df, "name", report)

        assert len(report.raised_errors) == 2
        assert "check_se_not_missing_and_in_range" in [
            err.check_data_id[0] for err in report.raised_errors]
        assert "check_se_0_when_val_0" in [
            err.check_data_id[0] for err in report.raised_errors]


class TestCheckBadN:
    params = {"data_source": "", "span_length": 1,
              "end_date": "2020-09-02", "expected_lag": {}}

    def test_empty_df(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        empty_df = pd.DataFrame(
            columns=["val", "se", "sample_size"], dtype=float)
        validator.check_bad_sample_size(empty_df, "", report)

        assert len(report.raised_errors) == 0

        validator.missing_sample_size_allowed = True
        validator.check_bad_sample_size(empty_df, "", report)

        assert len(report.raised_errors) == 0

    def test_missing(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        validator.missing_sample_size_allowed = True
        df = pd.DataFrame([[np.nan, np.nan, np.nan]], columns=[
                          "val", "se", "sample_size"])
        validator.check_bad_sample_size(df, "name", report)

        assert len(report.raised_errors) == 0

        validator.missing_sample_size_allowed = False
        validator.check_bad_sample_size(df, "name", report)

        assert len(report.raised_errors) == 1
        assert "check_n_missing" in [
            err.check_data_id[0] for err in report.raised_errors]

    def test_lt_min_missing_allowed(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        validator.missing_sample_size_allowed = True
        df = pd.DataFrame([[1, 0, 10], [1, np.nan, np.nan], [
                          1, np.nan, np.nan]], columns=["val", "se", "sample_size"])
        validator.check_bad_sample_size(df, "name", report)

        assert len(report.raised_errors) == 1
        assert "check_n_missing_or_gt_min" in [
            err.check_data_id[0] for err in report.raised_errors]

    def test_lt_min_missing_not_allowed(self):
        validator = StaticValidation(self.params)
        report = ValidationReport(validator.suppressed_errors)
        validator.missing_sample_size_allowed = False
        df = pd.DataFrame([[1, 0, 10], [1, np.nan, 240], [
                          1, np.nan, 245]], columns=["val", "se", "sample_size"])
        validator.check_bad_sample_size(df, "name", report)

        assert len(report.raised_errors) == 1
        assert "check_n_gt_min" in [
            err.check_data_id[0] for err in report.raised_errors]

