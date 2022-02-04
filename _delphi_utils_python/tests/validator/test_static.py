"""Tests for static validation."""
import numpy as np
import pandas as pd

from delphi_utils.validator.datafetcher import FILENAME_REGEX
from delphi_utils.validator.report import ValidationReport
from delphi_utils.validator.static import StaticValidator

# Properly formatted file name to use in tests where the actual value doesn't matter.
FILENAME = "17760704_nation_num_declarations.csv"

class TestCheckMissingDates:

    def test_empty_filelist(self):
        params = {
            "common": {
                "data_source": "",
                "span_length": 8,
                "end_date": "2020-09-09",
                "max_expected_lag": {"all": "1"}
            }
        }
        validator = StaticValidator(params)
        report = ValidationReport([])
        report = ValidationReport([])

        filenames = list()
        validator.check_missing_date_files(filenames, report)

        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_missing_date_files"

    def test_same_day(self):
        params = {
            "common": {
                "data_source": "",
                "span_length": 0,
                "end_date": "2020-09-01"
            }
        }
        validator = StaticValidator(params)
        report = ValidationReport([])

        filenames = [("20200901_county_signal_signal.csv", "match_obj")]
        validator.check_missing_date_files(filenames, report)

        assert len(report.raised_errors) == 0

    def test_duplicate_dates(self):
        params = {
            "common": {
                "data_source": "",
                "span_length": 1,
                "end_date": "2020-09-02",
                "max_expected_lag": {"all": "0"}
            }
        }
        validator = StaticValidator(params)
        report = ValidationReport([])

        filenames = [("20200901_county_signal_signal.csv", "match_obj"),
                     ("20200903_county_signal_signal.csv", "match_obj"),
                     ("20200903_usa_signal_signal.csv", "match_obj"),
                     ("20200903_usa_signal_signal.csv", "match_obj")]
        validator.check_missing_date_files(filenames, report)

        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_missing_date_files"


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
    params = {
        "common": {
            "data_source": "",
            "span_length": 0,
            "end_date": "2020-09-02"
        }
    }

    def test_empty_df(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        empty_df = pd.DataFrame(columns=["geo_id"], dtype=str)
        validator.check_bad_geo_id_format(empty_df, FILENAME, "county", report)

        assert len(report.raised_errors) == 0

    def test_invalid_geo_type(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        empty_df = pd.DataFrame(columns=["geo_id"], dtype=str)
        validator.check_bad_geo_id_format(empty_df, FILENAME, "hello", report)

        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_geo_type"

    def test_invalid_geo_id_format_county(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        df = pd.DataFrame(["0", "54321", "123", ".0000",
                           "abc12"], columns=["geo_id"])
        validator.check_bad_geo_id_format(df, FILENAME, "county", report)

        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_geo_id_format"

    def test_invalid_geo_id_format_msa(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        df = pd.DataFrame(["0", "54321", "123", ".0000",
                           "abc12"], columns=["geo_id"])
        validator.check_bad_geo_id_format(df, FILENAME, "msa", report)

        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_geo_id_format"

    def test_invalid_geo_id_format_hrr(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        df = pd.DataFrame(["1", "12", "123", "1234", "12345",
                           "a", ".", "ab1"], columns=["geo_id"])
        validator.check_bad_geo_id_format(df, FILENAME, "hrr", report)

        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_geo_id_format"

    def test_invalid_geo_id_format_state(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        df = pd.DataFrame(["aa", "hi", "HI", "hawaii",
                           "Hawaii", "a", "H.I."], columns=["geo_id"])
        validator.check_bad_geo_id_format(df, FILENAME, "state", report)

        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_geo_id_format"

    def test_invalid_geo_id_format_hhs(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        df = pd.DataFrame(["1", "112"], columns=["geo_id"])
        validator.check_bad_geo_id_format(df, FILENAME, "hhs", report)

        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_geo_id_format"

    def test_invalid_geo_id_format_nation(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        df = pd.DataFrame(["usa", "SP", " us", "us",
                           "usausa", "US"], columns=["geo_id"])
        validator.check_bad_geo_id_format(df, FILENAME, "nation", report)

        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_geo_id_format"

class TestDuplicatedRows:
    params = {
        "common": {
            "data_source": "",
            "span_length": 0,
            "end_date": "2020-09-02"
        }
    }

    def test_no_duplicates(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        df = pd.DataFrame([["a", "1"], ["b", "2"], ["c", "3"]])
        validator.check_duplicate_rows(df, FILENAME, report)
        assert len(report.raised_warnings) == 0

    def test_single_column_duplicates_but_not_row(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        df = pd.DataFrame([["a", "1"], ["a", "2"], ["b", "2"]])
        validator.check_duplicate_rows(df, FILENAME, report)
        assert len(report.raised_warnings) == 0

    def test_non_consecutive_duplicates(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        df = pd.DataFrame([["a", "1"], ["b", "2"], ["a", "1"]])
        validator.check_duplicate_rows(df, FILENAME, report)
        assert len(report.raised_warnings) == 1
        assert report.raised_warnings[0].check_name == "check_duplicate_rows"

    def test_multiple_distinct_duplicates(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        df = pd.DataFrame([["a", "1"], ["b", "2"], ["a", "1"], ["b", "2"]])
        validator.check_duplicate_rows(df, FILENAME, report)
        assert len(report.raised_warnings) == 1
        assert report.raised_warnings[0].check_name == "check_duplicate_rows"

    def test_more_than_two_copies(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        df = pd.DataFrame([["a", "1"], ["b", "2"], ["b", "2"], ["b", "2"]])
        validator.check_duplicate_rows(df, FILENAME, report)
        assert len(report.raised_warnings) == 1
        assert report.raised_warnings[0].check_name == "check_duplicate_rows"

class TestCheckBadGeoIdValue:
    params = {
        "common": {
            "data_source": "",
            "span_length": 0,
            "end_date": "2020-09-02",
        }
    }

    def test_empty_df(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        empty_df = pd.DataFrame(columns=["geo_id"], dtype=str)
        validator.check_bad_geo_id_value(empty_df, FILENAME, "county", report)
        assert len(report.raised_errors) == 0

    def test_state_level_fips(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        df = pd.DataFrame(["37183", "56000", "04000", "60000", "78000"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, FILENAME, "county", report)

        assert len(report.raised_errors) == 0

        df = pd.DataFrame(["37183", "56000", "04000", "60000", "78000", "99000"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, FILENAME, "county", report)

        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_bad_geo_id_value"

    def test_invalid_geo_id_value_county(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        df = pd.DataFrame(["01001", "88888", "99999"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, FILENAME, "county", report)

        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_bad_geo_id_value"

    def test_invalid_geo_id_value_msa(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        df = pd.DataFrame(["10180", "88888", "99999"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, FILENAME, "msa", report)

        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_bad_geo_id_value"

    def test_invalid_geo_id_value_hrr(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        df = pd.DataFrame(["1", "11", "111", "8", "88",
                           "888"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, FILENAME, "hrr", report)

        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_bad_geo_id_value"

    def test_invalid_geo_id_value_hhs(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        df = pd.DataFrame(["1", "11"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, FILENAME, "hhs", report)

        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_bad_geo_id_value"

    def test_invalid_geo_id_value_state(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        df = pd.DataFrame(["aa", "ak"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, FILENAME, "state", report)

        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_bad_geo_id_value"

    def test_uppercase_geo_id(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        df = pd.DataFrame(["ak", "AK"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, FILENAME, "state", report)

        assert len(report.raised_errors) == 0
        assert len(report.raised_warnings) == 1
        assert report.raised_warnings[0].check_name == "check_geo_id_lowercase"

    def test_invalid_geo_id_value_nation(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        df = pd.DataFrame(["us", "zz"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, FILENAME, "nation", report)

        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_bad_geo_id_value"

    def test_additional_valid_geo_ids(self):
        params = self.params.copy()
        params["static"] = {
            "additional_valid_geo_values": {
                    "state": ["state1"],
                    "county": ["county1", "county2"]
            }
        }
        validator = StaticValidator(params)
        report = ValidationReport([])

        df = pd.DataFrame(["05109", "06019", "county2"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, FILENAME, "county", report)
        assert len(report.raised_errors) == 0

        df = pd.DataFrame(["ma", "state1", "mi"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, FILENAME, "state", report)
        assert len(report.raised_errors) == 0

        df = pd.DataFrame(["county2", "02"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, FILENAME, "hhs", report)
        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_bad_geo_id_value"


class TestCheckBadVal:
    params = {
        "common": {
            "data_source": "",
            "span_length": 1,
            "end_date": "2020-09-02"
        }
    }

    def test_empty_df(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        empty_df = pd.DataFrame(columns=["val"])
        validator.check_bad_val(empty_df, "", "", report)
        validator.check_bad_val(empty_df, "", "prop", report)
        validator.check_bad_val(empty_df, "", "pct", report)

        assert len(report.raised_errors) == 0

    def test_lt_0(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        df = pd.DataFrame([-5], columns=["val"])
        validator.check_bad_val(df, FILENAME, "signal", report)

        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_val_lt_0"

    def test_gt_max_pct(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        df = pd.DataFrame([1e7], columns=["val"])
        validator.check_bad_val(df, FILENAME, "pct", report)

        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_val_pct_gt_100"

    def test_gt_max_prop(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        df = pd.DataFrame([1e7], columns=["val"])
        validator.check_bad_val(df, FILENAME, "prop", report)

        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_val_prop_gt_100k"


class TestCheckBadSe:
    params = {
        "common": {
            "data_source": "",
            "span_length": 1,
            "end_date": "2020-09-02"
        }
    }

    def test_empty_df(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        empty_df = pd.DataFrame(
            columns=["val", "se", "sample_size"], dtype=float)
        validator.check_bad_se(empty_df, "", report)

        assert len(report.raised_errors) == 0

        validator.params.missing_se_allowed = True
        validator.check_bad_se(empty_df, "", report)

        assert len(report.raised_errors) == 0

    def test_missing(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        validator.params.missing_se_allowed = True
        df = pd.DataFrame([[np.nan, np.nan, np.nan]], columns=[
                          "val", "se", "sample_size"])
        validator.check_bad_se(df, FILENAME, report)

        assert len(report.raised_errors) == 0

        validator.params.missing_se_allowed = False
        validator.check_bad_se(df, FILENAME, report)

        assert len(report.raised_errors) == 2
        assert "check_se_not_missing_and_in_range" in [
            err.check_name for err in report.raised_errors]
        assert "check_se_many_missing" in [
            err.check_name for err in report.raised_errors]

    def test_e_0_neg_missing_allowed(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        validator.params.missing_se_allowed = True
        df = pd.DataFrame([[1, -1, 200], [1, 0, np.nan], [
                          1, np.nan, np.nan]], columns=["val", "se", "sample_size"])
        validator.check_bad_se(df, FILENAME, report)

        assert len(report.raised_errors) == 2
        assert "check_se_missing_or_in_range" in [
            err.check_name for err in report.raised_errors]
        assert "check_se_0" in [
            err.check_name for err in report.raised_errors]

    def test_e_0_missing_not_allowed(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        validator.params.missing_se_allowed = False
        df = pd.DataFrame([[1, 0, 200], [1, 0, np.nan], [
                          1, np.nan, np.nan]], columns=["val", "se", "sample_size"])
        validator.check_bad_se(df, FILENAME, report)

        assert len(report.raised_errors) == 2
        assert "check_se_not_missing_and_in_range" in [
            err.check_name for err in report.raised_errors]
        assert "check_se_0" in [
            err.check_name for err in report.raised_errors]

    def test_jeffreys(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        validator.params.missing_se_allowed = False
        df = pd.DataFrame([[0, 0, 200], [1, 0, np.nan], [
                          1, np.nan, np.nan]], columns=["val", "se", "sample_size"])
        validator.check_bad_se(df, FILENAME, report)

        assert len(report.raised_errors) == 2
        assert "check_se_not_missing_and_in_range" in [
            err.check_name for err in report.raised_errors]
        assert "check_se_0_when_val_0" in [
            err.check_name for err in report.raised_errors]


class TestCheckBadN:
    params = {
        "common": {
            "data_source": "",
            "span_length": 1,
            "end_date": "2020-09-02"
        }
    }

    def test_empty_df(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        empty_df = pd.DataFrame(
            columns=["val", "se", "sample_size"], dtype=float)
        validator.check_bad_sample_size(empty_df, "", report)

        assert len(report.raised_errors) == 0

        validator.params.missing_sample_size_allowed = True
        validator.check_bad_sample_size(empty_df, "", report)

        assert len(report.raised_errors) == 0

    def test_missing(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        validator.params.missing_sample_size_allowed = True
        df = pd.DataFrame([[np.nan, np.nan, np.nan]], columns=[
                          "val", "se", "sample_size"])
        validator.check_bad_sample_size(df, FILENAME, report)

        assert len(report.raised_errors) == 0

        validator.params.missing_sample_size_allowed = False
        validator.check_bad_sample_size(df, FILENAME, report)

        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_n_missing"

    def test_lt_min_missing_allowed(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        validator.params.missing_sample_size_allowed = True
        df = pd.DataFrame([[1, 0, 10], [1, np.nan, np.nan], [
                          1, np.nan, np.nan]], columns=["val", "se", "sample_size"])
        validator.check_bad_sample_size(df, FILENAME, report)

        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_n_missing_or_gt_min"

    def test_lt_min_missing_not_allowed(self):
        validator = StaticValidator(self.params)
        report = ValidationReport([])
        validator.params.missing_sample_size_allowed = False
        df = pd.DataFrame([[1, 0, 10], [1, np.nan, 240], [
                          1, np.nan, 245]], columns=["val", "se", "sample_size"])
        validator.check_bad_sample_size(df, FILENAME, report)

        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_n_gt_min"
