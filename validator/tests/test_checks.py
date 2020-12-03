import pytest
from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd

from delphi_validator.datafetcher import filename_regex
from delphi_validator.validate import Validator, make_date_filter


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
        params = {"data_source": "", "span_length": 0,
                  "end_date": "2020-09-01", "expected_lag": {}}
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
        params = {"data_source": "", "span_length": 8,
                  "end_date": "2020-09-09", "expected_lag": {}}
        validator = Validator(params)

        filenames = list()
        validator.check_missing_date_files(filenames)

        assert len(validator.raised_errors) == 1
        assert "check_missing_date_files" in [
            err.check_data_id[0] for err in validator.raised_errors]
        assert len(validator.raised_errors[0].expression) == 9

    def test_same_day(self):
        params = {"data_source": "", "span_length": 0,
                  "end_date": "2020-09-01", "expected_lag": {}}
        validator = Validator(params)

        filenames = [("20200901_county_signal_signal.csv", "match_obj")]
        validator.check_missing_date_files(filenames)

        assert len(validator.raised_errors) == 0
        assert "check_missing_date_files" not in [
            err.check_data_id[0] for err in validator.raised_errors]

    def test_duplicate_dates(self):
        params = {"data_source": "", "span_length": 1,
                  "end_date": "2020-09-02", "expected_lag": {}}
        validator = Validator(params)

        filenames = [("20200901_county_signal_signal.csv", "match_obj"),
                     ("20200903_county_signal_signal.csv", "match_obj"),
                     ("20200903_usa_signal_signal.csv", "match_obj"),
                     ("20200903_usa_signal_signal.csv", "match_obj")]
        validator.check_missing_date_files(filenames)

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


class TestCheckBadGeoIdFormat:
    params = {"data_source": "", "span_length": 0,
              "end_date": "2020-09-02", "expected_lag": {}}

    def test_empty_df(self):
        validator = Validator(self.params)
        empty_df = pd.DataFrame(columns=["geo_id"], dtype=str)
        validator.check_bad_geo_id_format(empty_df, "name", "county")

        assert len(validator.raised_errors) == 0

    def test_invalid_geo_type(self):
        validator = Validator(self.params)
        empty_df = pd.DataFrame(columns=["geo_id"], dtype=str)
        validator.check_bad_geo_id_format(empty_df, "name", "hello")

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
        validator.check_bad_geo_id_format(df, "name", "county")

        assert len(validator.raised_errors) == 1
        assert "check_geo_id_format" in validator.raised_errors[0].check_data_id
        assert len(validator.raised_errors[0].expression) == 2
        assert "54321" not in validator.raised_errors[0].expression

    def test_invalid_geo_id_msa(self):
        validator = Validator(self.params)
        df = pd.DataFrame(["0", "54321", "123", ".0000",
                           "abc12"], columns=["geo_id"])
        validator.check_bad_geo_id_format(df, "name", "msa")

        assert len(validator.raised_errors) == 1
        assert "check_geo_id_format" in validator.raised_errors[0].check_data_id
        assert len(validator.raised_errors[0].expression) == 2
        assert "54321" not in validator.raised_errors[0].expression

    def test_invalid_geo_id_hrr(self):
        validator = Validator(self.params)
        df = pd.DataFrame(["1", "12", "123", "1234", "12345",
                           "a", ".", "ab1"], columns=["geo_id"])
        validator.check_bad_geo_id_format(df, "name", "hrr")

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
        validator.check_bad_geo_id_format(df, "name", "state")

        assert len(validator.raised_errors) == 1
        assert "check_geo_id_format" in validator.raised_errors[0].check_data_id
        assert len(validator.raised_errors[0].expression) == 4
        assert "aa" not in validator.raised_errors[0].expression
        assert "hi" not in validator.raised_errors[0].expression
        assert "HI" not in validator.raised_errors[0].expression

    def test_invalid_geo_id_national(self):
        validator = Validator(self.params)
        df = pd.DataFrame(["usa", "SP", " us", "us",
                           "usausa", "US"], columns=["geo_id"])
        validator.check_bad_geo_id_format(df, "name", "national")

        assert len(validator.raised_errors) == 1
        assert "check_geo_id_format" in validator.raised_errors[0].check_data_id
        assert len(validator.raised_errors[0].expression) == 3
        assert "us" not in validator.raised_errors[0].expression
        assert "US" not in validator.raised_errors[0].expression
        assert "SP" not in validator.raised_errors[0].expression

class TestDuplicatedRows:
    params = {"data_source": "", "span_length": 1,
              "end_date": "2020-09-02", "expected_lag": {}}
    def test_no_duplicates(self):
        validator = Validator(self.params)
        df = pd.DataFrame([["a", "1"], ["b", "2"], ["c", "3"]])
        validator.check_duplicate_rows(df, "file")
        assert len(validator.raised_warnings) == 0

    def test_single_column_duplicates_but_not_row(self):
        validator = Validator(self.params)
        df = pd.DataFrame([["a", "1"], ["a", "2"], ["b", "2"]])
        validator.check_duplicate_rows(df, "file")
        assert len(validator.raised_warnings) == 0

    def test_non_consecutive_duplicates(self):
        validator = Validator(self.params)
        df = pd.DataFrame([["a", "1"], ["b", "2"], ["a", "1"]])
        validator.check_duplicate_rows(df, "file")
        assert len(validator.raised_warnings) == 1
        assert validator.raised_warnings[0].expression == [2]
        assert validator.raised_warnings[0].check_data_id[1] == "file"

    def test_multiple_distinct_duplicates(self):
        validator = Validator(self.params)
        df = pd.DataFrame([["a", "1"], ["b", "2"], ["a", "1"], ["b", "2"]])
        validator.check_duplicate_rows(df, "file")
        assert len(validator.raised_warnings) == 1
        assert validator.raised_warnings[0].expression == [2, 3]

    def test_more_than_two_copies(self):
        validator = Validator(self.params)
        df = pd.DataFrame([["a", "1"], ["b", "2"], ["b", "2"], ["b", "2"]])
        validator.check_duplicate_rows(df, "file")
        assert len(validator.raised_warnings) == 1
        assert validator.raised_warnings[0].expression == [2, 3]

class TestCheckBadGeoIdValue:
    params = {"data_source": "", "span_length": 0,
              "end_date": "2020-09-02", "expected_lag": {},
              "validator_static_file_dir": "../static"}

    def test_empty_df(self):
        validator = Validator(self.params)
        empty_df = pd.DataFrame(columns=["geo_id"], dtype=str)
        validator.check_bad_geo_id_value(empty_df, "name", "county")
        assert len(validator.raised_errors) == 0

    def test_invalid_geo_id_county(self):
        validator = Validator(self.params)
        df = pd.DataFrame(["01001", "88888", "99999"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, "name", "county")

        assert len(validator.raised_errors) == 1
        assert "check_bad_geo_id_value" in validator.raised_errors[0].check_data_id
        assert len(validator.raised_errors[0].expression) == 2
        assert "01001" not in validator.raised_errors[0].expression
        assert "88888" in validator.raised_errors[0].expression
        assert "99999" in validator.raised_errors[0].expression

    def test_invalid_geo_id_msa(self):
        validator = Validator(self.params)
        df = pd.DataFrame(["10180", "88888", "99999"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, "name", "msa")

        assert len(validator.raised_errors) == 1
        assert "check_bad_geo_id_value" in validator.raised_errors[0].check_data_id
        assert len(validator.raised_errors[0].expression) == 2
        assert "10180" not in validator.raised_errors[0].expression
        assert "88888" in validator.raised_errors[0].expression
        assert "99999" in validator.raised_errors[0].expression

    def test_invalid_geo_id_hrr(self):
        validator = Validator(self.params)
        df = pd.DataFrame(["1", "11", "111", "8", "88",
                           "888"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, "name", "hrr")

        assert len(validator.raised_errors) == 1
        assert "check_bad_geo_id_value" in validator.raised_errors[0].check_data_id
        assert len(validator.raised_errors[0].expression) == 3
        assert "1" not in validator.raised_errors[0].expression
        assert "11" not in validator.raised_errors[0].expression
        assert "111" not in validator.raised_errors[0].expression
        assert "8" in validator.raised_errors[0].expression
        assert "88" in validator.raised_errors[0].expression
        assert "888" in validator.raised_errors[0].expression

    def test_invalid_geo_id_state(self):
        validator = Validator(self.params)
        df = pd.DataFrame(["aa", "ak"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, "name", "state")

        assert len(validator.raised_errors) == 1
        assert "check_bad_geo_id_value" in validator.raised_errors[0].check_data_id
        assert len(validator.raised_errors[0].expression) == 1
        assert "ak" not in validator.raised_errors[0].expression
        assert "aa" in validator.raised_errors[0].expression

    def test_uppercase_geo_id(self):
        validator = Validator(self.params)
        df = pd.DataFrame(["ak", "AK"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, "name", "state")

        assert len(validator.raised_errors) == 0
        assert len(validator.raised_warnings) == 1
        assert "check_geo_id_lowercase" in validator.raised_warnings[0].check_data_id
        assert "AK" in validator.raised_warnings[0].expression

    def test_invalid_geo_id_national(self):
        validator = Validator(self.params)
        df = pd.DataFrame(["us", "zz"], columns=["geo_id"])
        validator.check_bad_geo_id_value(df, "name", "national")

        assert len(validator.raised_errors) == 1
        assert "check_bad_geo_id_value" in validator.raised_errors[0].check_data_id
        assert len(validator.raised_errors[0].expression) == 1
        assert "us" not in validator.raised_errors[0].expression
        assert "zz" in validator.raised_errors[0].expression


class TestCheckBadVal:
    params = {"data_source": "", "span_length": 1,
              "end_date": "2020-09-02", "expected_lag": {}}

    def test_empty_df(self):
        validator = Validator(self.params)
        empty_df = pd.DataFrame(columns=["val"])
        validator.check_bad_val(empty_df, "", "")
        validator.check_bad_val(empty_df, "", "prop")
        validator.check_bad_val(empty_df, "", "pct")

        assert len(validator.raised_errors) == 0

    def test_missing(self):
        validator = Validator(self.params)
        df = pd.DataFrame([np.nan], columns=["val"])
        validator.check_bad_val(df, "name", "signal")

        assert len(validator.raised_errors) == 1
        assert "check_val_missing" in validator.raised_errors[0].check_data_id

    def test_lt_0(self):
        validator = Validator(self.params)
        df = pd.DataFrame([-5], columns=["val"])
        validator.check_bad_val(df, "name", "signal")

        assert len(validator.raised_errors) == 1
        assert "check_val_lt_0" in validator.raised_errors[0].check_data_id

    def test_gt_max_pct(self):
        validator = Validator(self.params)
        df = pd.DataFrame([1e7], columns=["val"])
        validator.check_bad_val(df, "name", "pct")

        assert len(validator.raised_errors) == 1
        assert "check_val_pct_gt_100" in validator.raised_errors[0].check_data_id

    def test_gt_max_prop(self):
        validator = Validator(self.params)
        df = pd.DataFrame([1e7], columns=["val"])
        validator.check_bad_val(df, "name", "prop")

        assert len(validator.raised_errors) == 1
        assert "check_val_prop_gt_100k" in validator.raised_errors[0].check_data_id


class TestCheckBadSe:
    params = {"data_source": "", "span_length": 1,
              "end_date": "2020-09-02", "expected_lag": {}}

    def test_empty_df(self):
        validator = Validator(self.params)
        empty_df = pd.DataFrame(
            columns=["val", "se", "sample_size"], dtype=float)
        validator.check_bad_se(empty_df, "")

        assert len(validator.raised_errors) == 0

        validator.missing_se_allowed = True
        validator.check_bad_se(empty_df, "")

        assert len(validator.raised_errors) == 0

    def test_missing(self):
        validator = Validator(self.params)
        validator.missing_se_allowed = True
        df = pd.DataFrame([[np.nan, np.nan, np.nan]], columns=[
                          "val", "se", "sample_size"])
        validator.check_bad_se(df, "name")

        assert len(validator.raised_errors) == 0

        validator.missing_se_allowed = False
        validator.check_bad_se(df, "name")

        assert len(validator.raised_errors) == 2
        assert "check_se_not_missing_and_in_range" in [
            err.check_data_id[0] for err in validator.raised_errors]
        assert "check_se_many_missing" in [
            err.check_data_id[0] for err in validator.raised_errors]

    def test_e_0_missing_allowed(self):
        validator = Validator(self.params)
        validator.missing_se_allowed = True
        df = pd.DataFrame([[1, 0, 200], [1, np.nan, np.nan], [
                          1, np.nan, np.nan]], columns=["val", "se", "sample_size"])
        validator.check_bad_se(df, "name")

        assert len(validator.raised_errors) == 2
        assert "check_se_missing_or_in_range" in [
            err.check_data_id[0] for err in validator.raised_errors]
        assert "check_se_0" in [
            err.check_data_id[0] for err in validator.raised_errors]

    def test_e_0_missing_not_allowed(self):
        validator = Validator(self.params)
        validator.missing_se_allowed = False
        df = pd.DataFrame([[1, 0, 200], [1, 0, np.nan], [
                          1, np.nan, np.nan]], columns=["val", "se", "sample_size"])
        validator.check_bad_se(df, "name")

        assert len(validator.raised_errors) == 2
        assert "check_se_not_missing_and_in_range" in [
            err.check_data_id[0] for err in validator.raised_errors]
        assert "check_se_0" in [
            err.check_data_id[0] for err in validator.raised_errors]

    def test_jeffreys(self):
        validator = Validator(self.params)
        validator.missing_se_allowed = False
        df = pd.DataFrame([[0, 0, 200], [1, 0, np.nan], [
                          1, np.nan, np.nan]], columns=["val", "se", "sample_size"])
        validator.check_bad_se(df, "name")

        assert len(validator.raised_errors) == 2
        assert "check_se_not_missing_and_in_range" in [
            err.check_data_id[0] for err in validator.raised_errors]
        assert "check_se_0_when_val_0" in [
            err.check_data_id[0] for err in validator.raised_errors]


class TestCheckBadN:
    params = {"data_source": "", "span_length": 1,
              "end_date": "2020-09-02", "expected_lag": {}}

    def test_empty_df(self):
        validator = Validator(self.params)
        empty_df = pd.DataFrame(
            columns=["val", "se", "sample_size"], dtype=float)
        validator.check_bad_sample_size(empty_df, "")

        assert len(validator.raised_errors) == 0

        validator.missing_sample_size_allowed = True
        validator.check_bad_sample_size(empty_df, "")

        assert len(validator.raised_errors) == 0

    def test_missing(self):
        validator = Validator(self.params)
        validator.missing_sample_size_allowed = True
        df = pd.DataFrame([[np.nan, np.nan, np.nan]], columns=[
                          "val", "se", "sample_size"])
        validator.check_bad_sample_size(df, "name")

        assert len(validator.raised_errors) == 0

        validator.missing_sample_size_allowed = False
        validator.check_bad_sample_size(df, "name")

        assert len(validator.raised_errors) == 1
        assert "check_n_missing" in [
            err.check_data_id[0] for err in validator.raised_errors]

    def test_lt_min_missing_allowed(self):
        validator = Validator(self.params)
        validator.missing_sample_size_allowed = True
        df = pd.DataFrame([[1, 0, 10], [1, np.nan, np.nan], [
                          1, np.nan, np.nan]], columns=["val", "se", "sample_size"])
        validator.check_bad_sample_size(df, "name")

        assert len(validator.raised_errors) == 1
        assert "check_n_missing_or_gt_min" in [
            err.check_data_id[0] for err in validator.raised_errors]

    def test_lt_min_missing_not_allowed(self):
        validator = Validator(self.params)
        validator.missing_sample_size_allowed = False
        df = pd.DataFrame([[1, 0, 10], [1, np.nan, 240], [
                          1, np.nan, 245]], columns=["val", "se", "sample_size"])
        validator.check_bad_sample_size(df, "name")

        assert len(validator.raised_errors) == 1
        assert "check_n_gt_min" in [
            err.check_data_id[0] for err in validator.raised_errors]


class TestCheckRapidChange:
    params = {"data_source": "", "span_length": 1,
              "end_date": "2020-09-02", "expected_lag": {}}

    def test_same_df(self):
        validator = Validator(self.params)
        test_df = pd.DataFrame([date.today()] * 5, columns=["time_value"])
        ref_df = pd.DataFrame([date.today()] * 5, columns=["time_value"])
        validator.check_rapid_change_num_rows(
            test_df, ref_df, date.today(), "geo", "signal")

        assert len(validator.raised_errors) == 0

    def test_0_vs_many(self):
        validator = Validator(self.params)

        time_value = datetime.combine(date.today(), datetime.min.time())

        test_df = pd.DataFrame([time_value] * 5, columns=["time_value"])
        ref_df = pd.DataFrame([time_value] * 1, columns=["time_value"])
        validator.check_rapid_change_num_rows(
            test_df, ref_df, time_value, "geo", "signal")

        assert len(validator.raised_errors) == 1
        assert "check_rapid_change_num_rows" in [
            err.check_data_id[0] for err in validator.raised_errors]


class TestCheckAvgValDiffs:
    params = {"data_source": "", "span_length": 1,
              "end_date": "2020-09-02", "expected_lag": {}}

    def test_same_val(self):
        validator = Validator(self.params)

        data = {"val": [1, 1, 1, 2, 0, 1], "se": [np.nan] * 6,
                "sample_size": [np.nan] * 6, "geo_id": ["1"] * 6}

        test_df = pd.DataFrame(data)
        ref_df = pd.DataFrame(data)

        validator.check_avg_val_vs_reference(
            test_df, ref_df, date.today(), "geo", "signal")

        assert len(validator.raised_errors) == 0

    def test_same_se(self):
        validator = Validator(self.params)

        data = {"val": [np.nan] * 6, "se": [1, 1, 1, 2, 0, 1],
                "sample_size": [np.nan] * 6, "geo_id": ["1"] * 6}

        test_df = pd.DataFrame(data)
        ref_df = pd.DataFrame(data)

        validator.check_avg_val_vs_reference(
            test_df, ref_df, date.today(), "geo", "signal")

        assert len(validator.raised_errors) == 0

    def test_same_n(self):
        validator = Validator(self.params)

        data = {"val": [np.nan] * 6, "se": [np.nan] * 6,
                "sample_size": [1, 1, 1, 2, 0, 1], "geo_id": ["1"] * 6}

        test_df = pd.DataFrame(data)
        ref_df = pd.DataFrame(data)

        validator.check_avg_val_vs_reference(
            test_df, ref_df, date.today(), "geo", "signal")

        assert len(validator.raised_errors) == 0

    def test_same_val_se_n(self):
        validator = Validator(self.params)

        data = {"val": [1, 1, 1, 2, 0, 1], "se": [1, 1, 1, 2, 0, 1],
                "sample_size": [1, 1, 1, 2, 0, 1], "geo_id": ["1"] * 6}

        test_df = pd.DataFrame(data)
        ref_df = pd.DataFrame(data)

        validator.check_avg_val_vs_reference(
            test_df, ref_df, date.today(), "geo", "signal")

        assert len(validator.raised_errors) == 0

    def test_10x_val(self):
        validator = Validator(self.params)
        test_data = {"val": [1, 1, 1, 20, 0, 1], "se": [np.nan] * 6,
                     "sample_size": [np.nan] * 6, "geo_id": ["1"] * 6}
        ref_data = {"val": [1, 1, 1, 2, 0, 1], "se": [np.nan] * 6,
                    "sample_size": [np.nan] * 6, "geo_id": ["1"] * 6}

        test_df = pd.DataFrame(test_data)
        ref_df = pd.DataFrame(ref_data)
        validator.check_avg_val_vs_reference(
            test_df, ref_df,
            datetime.combine(date.today(), datetime.min.time()), "geo", "signal")

        assert len(validator.raised_errors) == 0

    def test_100x_val(self):
        validator = Validator(self.params)
        test_data = {"val": [1, 1, 1, 200, 0, 1], "se": [np.nan] * 6,
                     "sample_size": [np.nan] * 6, "geo_id": ["1"] * 6}
        ref_data = {"val": [1, 1, 1, 2, 0, 1], "se": [np.nan] * 6,
                    "sample_size": [np.nan] * 6, "geo_id": ["1"] * 6}

        test_df = pd.DataFrame(test_data)
        ref_df = pd.DataFrame(ref_data)
        validator.check_avg_val_vs_reference(
            test_df, ref_df,
            datetime.combine(date.today(), datetime.min.time()), "geo", "signal")

        assert len(validator.raised_errors) == 1
        assert "check_test_vs_reference_avg_changed" in [
            err.check_data_id[0] for err in validator.raised_errors]

    def test_1000x_val(self):
        validator = Validator(self.params)
        test_data = {"val": [1, 1, 1, 2000, 0, 1], "se": [np.nan] * 6,
                     "sample_size": [np.nan] * 6, "geo_id": ["1"] * 6}
        ref_data = {"val": [1, 1, 1, 2, 0, 1], "se": [np.nan] * 6,
                    "sample_size": [np.nan] * 6, "geo_id": ["1"] * 6}

        test_df = pd.DataFrame(test_data)
        ref_df = pd.DataFrame(ref_data)
        validator.check_avg_val_vs_reference(
            test_df, ref_df,
            datetime.combine(date.today(), datetime.min.time()), "geo", "signal")

        assert len(validator.raised_errors) == 1
        assert "check_test_vs_reference_avg_changed" in [
            err.check_data_id[0] for err in validator.raised_errors]


class TestDataOutlier:
    params = {"data_source": "", "span_length": 1,
          "end_date": "2020-09-02", "expected_lag": {}}
    pd.set_option("display.max_rows", None, "display.max_columns", None)
    # Test to determine outliers based on the row data, has lead and lag outlier
    def test_pos_outlier(self):
        validator = Validator(self.params)

        ref_val = [30, 30.28571429, 30.57142857, 30.85714286, 31.14285714,
                31.42857143, 31.71428571, 32, 32, 32.14285714,
                32.28571429, 32.42857143, 32.57142857, 32.71428571,
                32.85714286, 33, 33, 33, 33, 33, 33, 33, 33,
                33, 33, 33, 33.28571429, 33.57142857, 33.85714286, 34.14285714]
        test_val = [100, 100, 100]


        ref_data = {"val": ref_val , "se": [np.nan] * len(ref_val),
                "sample_size": [np.nan] * len(ref_val), "geo_id": ["1"] * len(ref_val),
                "time_value": pd.date_range(start="2020-09-24", end="2020-10-23")}
        test_data = {"val": test_val , "se": [np.nan] * len(test_val),
                "sample_size": [np.nan] * len(test_val), "geo_id": ["1"] * len(test_val),
                 "time_value": pd.date_range(start="2020-10-24", end="2020-10-26")}

        ref_data2 = {"val": ref_val , "se": [np.nan] * len(ref_val),
                "sample_size": [np.nan] * len(ref_val), "geo_id": ["2"] * len(ref_val),
                "time_value": pd.date_range(start="2020-09-24", end="2020-10-23")}
        test_data2 = {"val": test_val , "se": [np.nan] * len(test_val),
                "sample_size": [np.nan] * len(test_val), "geo_id": ["2"] * len(test_val),
                "time_value": pd.date_range(start="2020-10-24",end="2020-10-26")}

        ref_df = pd.concat([pd.DataFrame(ref_data), pd.DataFrame(ref_data2)]).reset_index(drop=True)
        test_df = pd.concat([pd.DataFrame(test_data), pd.DataFrame(test_data2)]). \
                            reset_index(drop=True)

        validator.check_positive_negative_spikes(
            test_df, ref_df, "state", "signal")


        assert len(validator.raised_errors) == 1
        assert "check_positive_negative_spikes" in [
        err.check_data_id[0] for err in validator.raised_errors]

    def test_neg_outlier(self):
        validator = Validator(self.params)

        ref_val = [100, 101, 100, 101, 100,
        100, 100, 100, 100, 100,
        100, 102, 100, 100, 100,
        100, 100, 101, 100, 100,
        100, 100, 100, 99, 100,
        100, 98, 100, 100, 100]
        test_val = [10, 10, 10]


        ref_data = {"val": ref_val , "se": [np.nan] * len(ref_val),
                "sample_size": [np.nan] * len(ref_val), "geo_id": ["1"] * len(ref_val),
                "time_value": pd.date_range(start="2020-09-24",end="2020-10-23")}
        test_data = {"val": test_val , "se": [np.nan] * len(test_val),
                "sample_size": [np.nan] * len(test_val), "geo_id": ["1"] * len(test_val),
                 "time_value": pd.date_range(start="2020-10-24",end="2020-10-26")}

        ref_data2 = {"val": ref_val , "se": [np.nan] * len(ref_val),
                "sample_size": [np.nan] * len(ref_val), "geo_id": ["2"] * len(ref_val),
                 "time_value": pd.date_range(start="2020-09-24",end="2020-10-23")}
        test_data2 = {"val": test_val , "se": [np.nan] * len(test_val),
                "sample_size": [np.nan] * len(test_val), "geo_id": ["2"] * len(test_val),
                 "time_value": pd.date_range(start="2020-10-24",end="2020-10-26")}

        ref_df = pd.concat([pd.DataFrame(ref_data), pd.DataFrame(ref_data2)]). \
                    reset_index(drop=True)
        test_df = pd.concat([pd.DataFrame(test_data), pd.DataFrame(test_data2)]). \
                    reset_index(drop=True)


        validator.check_positive_negative_spikes(
            test_df, ref_df, "state", "signal")


        assert len(validator.raised_errors) == 1
        assert "check_positive_negative_spikes" in [
        err.check_data_id[0] for err in validator.raised_errors]

    def test_zero_outlier(self):
        validator = Validator(self.params)

        ref_val = [30, 30.28571429, 30.57142857, 30.85714286, 31.14285714,
                31.42857143, 31.71428571, 32, 32, 32.14285714,
                32.28571429, 32.42857143, 32.57142857, 32.71428571,
                32.85714286, 33, 33, 33, 33, 33, 33, 33, 33,
                33, 33, 33, 33.28571429, 33.57142857, 33.85714286, 34.14285714]
        test_val = [0, 0, 0]


        ref_data = {"val": ref_val , "se": [np.nan] * len(ref_val),
                "sample_size": [np.nan] * len(ref_val), "geo_id": ["1"] * len(ref_val),
                "time_value": pd.date_range(start="2020-09-24",end="2020-10-23")}
        test_data = {"val": test_val , "se": [np.nan] * len(test_val),
                "sample_size": [np.nan] * len(test_val), "geo_id": ["1"] * len(test_val),
                 "time_value": pd.date_range(start="2020-10-24",end="2020-10-26")}

        ref_data2 = {"val": ref_val , "se": [np.nan] * len(ref_val),
                "sample_size": [np.nan] * len(ref_val), "geo_id": ["2"] * len(ref_val),
                 "time_value": pd.date_range(start="2020-09-24",end="2020-10-23")}
        test_data2 = {"val": test_val , "se": [np.nan] * len(test_val),
                "sample_size": [np.nan] * len(test_val), "geo_id": ["2"] * len(test_val),
                 "time_value": pd.date_range(start="2020-10-24",end="2020-10-26")}

        ref_df = pd.concat([pd.DataFrame(ref_data), pd.DataFrame(ref_data2)]). \
                    reset_index(drop=True)
        test_df = pd.concat([pd.DataFrame(test_data), pd.DataFrame(test_data2)]). \
                    reset_index(drop=True)


        validator.check_positive_negative_spikes(
            test_df, ref_df, "state", "signal")



        assert len(validator.raised_errors) == 1
        assert "check_positive_negative_spikes" in [
        err.check_data_id[0] for err in validator.raised_errors]

    def test_no_outlier(self):
        validator = Validator(self.params)

        #Data from 51580 between 9/24 and 10/26 (10/25 query date)
        ref_val = [30, 30.28571429, 30.57142857, 30.85714286, 31.14285714,
                31.42857143, 31.71428571, 32, 32, 32.14285714,
                32.28571429, 32.42857143, 32.57142857, 32.71428571,
                32.85714286, 33, 33, 33, 33, 33, 33, 33, 33,
                33, 33, 33, 33, 33, 33, 33]
        test_val = [33, 33, 33]


        ref_data = {"val": ref_val , "se": [np.nan] * len(ref_val),
                "sample_size": [np.nan] * len(ref_val), "geo_id": ["1"] * len(ref_val),
                "time_value": pd.date_range(start="2020-09-24",end="2020-10-23")}
        test_data = {"val": test_val , "se": [np.nan] * len(test_val),
                "sample_size": [np.nan] * len(test_val), "geo_id": ["1"] * len(test_val),
                 "time_value": pd.date_range(start="2020-10-24",end="2020-10-26")}

        ref_data2 = {"val": ref_val , "se": [np.nan] * len(ref_val),
                "sample_size": [np.nan] * len(ref_val), "geo_id": ["2"] * len(ref_val),
                 "time_value": pd.date_range(start="2020-09-24",end="2020-10-23")}
        test_data2 = {"val": test_val , "se": [np.nan] * len(test_val),
                "sample_size": [np.nan] * len(test_val), "geo_id": ["2"] * len(test_val),
                 "time_value": pd.date_range(start="2020-10-24",end="2020-10-26")}

        ref_df = pd.concat([pd.DataFrame(ref_data), pd.DataFrame(ref_data2)]). \
                    reset_index(drop=True)
        test_df = pd.concat([pd.DataFrame(test_data), pd.DataFrame(test_data2)]). \
                    reset_index(drop=True)


        validator.check_positive_negative_spikes(
            test_df, ref_df, "state", "signal")


        assert len(validator.raised_errors) == 0

    def test_source_api_overlap(self):
        validator = Validator(self.params)

        #Data from 51580 between 9/24 and 10/26 (10/25 query date)
        ref_val = [30, 30.28571429, 30.57142857, 30.85714286, 31.14285714,
                31.42857143, 31.71428571, 32, 32, 32.14285714,
                32.28571429, 32.42857143, 32.57142857, 32.71428571,
                32.85714286, 33, 33, 33, 33, 33, 33, 33, 33, 33,
                33, 33, 33, 33, 33, 33, 33, 33, 33]
        test_val = [100, 100, 100]


        ref_data = {"val": ref_val , "se": [np.nan] * len(ref_val),
                "sample_size": [np.nan] * len(ref_val), "geo_id": ["1"] * len(ref_val),
                "time_value": pd.date_range(start="2020-09-24",end="2020-10-26")}
        test_data = {"val": test_val , "se": [np.nan] * len(test_val),
                "sample_size": [np.nan] * len(test_val), "geo_id": ["1"] * len(test_val),
                 "time_value": pd.date_range(start="2020-10-24",end="2020-10-26")}

        ref_data2 = {"val": ref_val , "se": [np.nan] * len(ref_val),
                "sample_size": [np.nan] * len(ref_val), "geo_id": ["2"] * len(ref_val),
                 "time_value": pd.date_range(start="2020-09-24",end="2020-10-26")}
        test_data2 = {"val": test_val , "se": [np.nan] * len(test_val),
                "sample_size": [np.nan] * len(test_val), "geo_id": ["2"] * len(test_val),
                 "time_value": pd.date_range(start="2020-10-24",end="2020-10-26")}

        ref_df = pd.concat([pd.DataFrame(ref_data), pd.DataFrame(ref_data2)]). \
                    reset_index(drop=True)
        test_df = pd.concat([pd.DataFrame(test_data), pd.DataFrame(test_data2)]). \
                    reset_index(drop=True)


        validator.check_positive_negative_spikes(
            test_df, ref_df, "state", "signal")


        assert len(validator.raised_errors) == 1
        assert "check_positive_negative_spikes" in [
        err.check_data_id[0] for err in validator.raised_errors]
