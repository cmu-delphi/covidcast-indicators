"""Tests for dynamic validator."""
from datetime import date, datetime
import numpy as np
import pandas as pd

from delphi_utils.validator.report import ValidationReport
from delphi_utils.validator.dynamic import DynamicValidator

class TestReferencePadding:
    params = {
        "common": {
            "data_source": "",
            "span_length": 1,
            "end_date": "2020-09-02"
        }
    }

    def test_no_padding(self):
        validator = DynamicValidator(self.params)
        report = ValidationReport([])
        data = {"val": [1, 1, 1, 2, 0, 1], "se": [np.nan] * 6,
                "sample_size": [np.nan] * 6, "geo_id": ["1"] * 6,
                "time_value": pd.date_range(start="2021-01-01", end="2021-01-06")}

        test_df = pd.DataFrame(data)
        ref_df = pd.DataFrame(data)

        ref_date = datetime.strptime("2021-01-06", "%Y-%m-%d").date()
        new_ref_df = validator.pad_reference_api_df(
            ref_df, test_df, ref_date, ref_date)

        assert new_ref_df.equals(ref_df)

    def test_half_padding(self):
        validator = DynamicValidator(self.params)
        report = ValidationReport([])
        ref_data = {"val": [2, 2, 2, 2, 2, 2], "se": [np.nan] * 6,
                "sample_size": [np.nan] * 6, "geo_id": ["1"] * 6,
                "time_value": pd.date_range(start="2021-01-01", end="2021-01-06")}
        test_data = {"val": [1, 1, 1, 1, 1, 1], "se": [np.nan] * 6,
                "sample_size": [np.nan] * 6, "geo_id": ["1"] * 6,
                "time_value": pd.date_range(start="2021-01-06", end="2021-01-11")}
        ref_df = pd.DataFrame(ref_data)
        test_df = pd.DataFrame(test_data)

        ref_date = datetime.strptime("2021-01-15", "%Y-%m-%d").date()
        new_ref_df = validator.pad_reference_api_df(
            ref_df, test_df, ref_date, ref_date)

        # Check it only takes missing dates - so the last 5 dates
        assert new_ref_df.time_value.max() == datetime.strptime("2021-01-11",
            "%Y-%m-%d").date()
        assert new_ref_df.shape[0] == 11
        assert new_ref_df["val"].iloc[5] == 2

    def test_full_padding(self):
        validator = DynamicValidator(self.params)
        report = ValidationReport([])
        ref_data = {"val": [2, 2, 2, 2, 2, 2], "se": [np.nan] * 6,
                "sample_size": [np.nan] * 6, "geo_id": ["1"] * 6,
                "time_value": pd.date_range(start="2021-01-01", end="2021-01-06")}
        test_data = {"val": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                     "se": [np.nan] * 12,
                "sample_size": [np.nan] * 12, "geo_id": ["1"] * 12,
                "time_value": pd.date_range(start="2021-01-06", end="2021-01-17")}
        ref_df = pd.DataFrame(ref_data)
        test_df = pd.DataFrame(test_data)

        ref_date = datetime.strptime("2021-01-15", "%Y-%m-%d").date()
        new_ref_df = validator.pad_reference_api_df(
            ref_df, test_df, ref_date, ref_date)

        # Check it only takes missing dates up to the day before the reference
        assert new_ref_df.time_value.max() == datetime.strptime("2021-01-15",
            "%Y-%m-%d").date()
        assert new_ref_df.shape[0] == 15
        assert new_ref_df["val"].iloc[5] == 2

class TestCheckRapidChange:
    params = {
        "common": {
            "data_source": "",
            "span_length": 1,
            "end_date": "2020-09-02"
        }
    }

    def test_same_df(self):
        validator = DynamicValidator(self.params)
        report = ValidationReport([])
        test_df = pd.DataFrame([date.today()] * 5, columns=["time_value"])
        ref_df = pd.DataFrame([date.today()] * 5, columns=["time_value"])
        validator.check_rapid_change_num_rows(
            test_df, ref_df, date.today(), "geo", "signal", report)

        assert len(report.raised_errors) == 0

    def test_0_vs_many(self):
        validator = DynamicValidator(self.params)
        report = ValidationReport([])

        time_value = datetime.combine(date.today(), datetime.min.time())

        test_df = pd.DataFrame([time_value] * 5, columns=["time_value"])
        ref_df = pd.DataFrame([time_value] * 1, columns=["time_value"])
        validator.check_rapid_change_num_rows(
            test_df, ref_df, time_value, "geo", "signal", report)

        assert len(report.raised_errors) == 1
        assert report.raised_errors[0].check_name == "check_rapid_change_num_rows"

class TestCheckNaVals:
    params = {
        "common": {
            "data_source": "",
            "span_length": 14,
            "end_date": "2020-09-02"
        }
    }
    def test_missing(self):
        validator = DynamicValidator(self.params)
        report = ValidationReport([])
        data = {"val": [np.nan] * 15, "geo_id": [0,1] * 7 + [2], 
            "time_value": ["2021-08-30"] * 14 + ["2021-05-01"]}
        df = pd.DataFrame(data)
        df.time_value = (pd.to_datetime(df.time_value)).dt.date
        validator.check_na_vals(df, "geo", "signal", report)

        assert len(report.raised_errors) == 2
        assert report.raised_errors[0].check_name == "check_val_missing"
        assert report.raised_errors[0].message == "geo_id 0"
        assert report.raised_errors[1].check_name == "check_val_missing"
        assert report.raised_errors[1].message == "geo_id 1"

class TestCheckAvgValDiffs:
    params = {
        "common": {
            "data_source": "",
            "span_length": 1,
            "end_date": "2020-09-02"
        }
    }

    def test_same_val(self):
        validator = DynamicValidator(self.params)
        report = ValidationReport([])

        data = {"val": [1, 1, 1, 2, 0, 1, 1]*2, "se": [np.nan] * 14,
                "sample_size": [np.nan] * 14, "geo_id": ["1"] * 14,
                "time_value": ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05", "2021-01-06", "2021-01-07",
                    "2021-01-08", "2021-01-09", "2021-01-10", "2021-01-11", "2021-01-12", "2021-01-13", "2021-01-14"]}

        test_df = pd.DataFrame(data)
        ref_df = pd.DataFrame(data)

        validator.check_avg_val_vs_reference(
            test_df, ref_df, date.today(), "geo", "signal", report)

        assert len(report.raised_errors) == 0

    def test_same_se(self):
        validator = DynamicValidator(self.params)
        report = ValidationReport([])

        data = {"val": [np.nan] * 14, "se": [1, 1, 1, 2, 0, 1, 1]*2,
                "sample_size": [np.nan] * 14, "geo_id": ["1"] * 14,
                "time_value": ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05", "2021-01-06", "2021-01-07",
                    "2021-01-08", "2021-01-09", "2021-01-10", "2021-01-11", "2021-01-12", "2021-01-13", "2021-01-14"]}

        test_df = pd.DataFrame(data)
        ref_df = pd.DataFrame(data)

        validator.check_avg_val_vs_reference(
            test_df, ref_df, date.today(), "geo", "signal", report)

        assert len(report.raised_errors) == 0

    def test_same_n(self):
        validator = DynamicValidator(self.params)
        report = ValidationReport([])

        data = {"val": [np.nan] * 14, "se": [np.nan] * 14,
                "sample_size": [1, 1, 1, 2, 0, 1, 1]*2, "geo_id": ["1"] * 14,
                "time_value": ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05", "2021-01-06", "2021-01-07",
                    "2021-01-08", "2021-01-09", "2021-01-10", "2021-01-11", "2021-01-12", "2021-01-13", "2021-01-14"]}

        test_df = pd.DataFrame(data)
        ref_df = pd.DataFrame(data)

        validator.check_avg_val_vs_reference(
            test_df, ref_df, date.today(), "geo", "signal", report)

        assert len(report.raised_errors) == 0

    def test_same_val_se_n(self):
        validator = DynamicValidator(self.params)
        report = ValidationReport([])

        data = {"val": [1, 1, 1, 2, 0, 1, 1]*2, "se": [1, 1, 1, 2, 0, 1, 1]*2,
                "sample_size": [1, 1, 1, 2, 0, 1, 1]*2, "geo_id": ["1"] * 14,
                "time_value": ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05", "2021-01-06", "2021-01-07",
                    "2021-01-08", "2021-01-09", "2021-01-10", "2021-01-11", "2021-01-12", "2021-01-13", "2021-01-14"]}

        test_df = pd.DataFrame(data)
        ref_df = pd.DataFrame(data)

        validator.check_avg_val_vs_reference(
            test_df, ref_df, date.today(), "geo", "signal", report)

        assert len(report.raised_errors) == 0

    def test_10x_val(self):
        validator = DynamicValidator(self.params)
        report = ValidationReport([])
        test_data = {"val": [1, 1, 1, 20, 0, 1, 1]*2, "se": [np.nan] * 14,
                     "sample_size": [np.nan] * 14, "geo_id": ["1"] * 14,
                     "time_value": ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05", "2021-01-06", "2021-01-07",
                        "2021-01-08", "2021-01-09", "2021-01-10", "2021-01-11", "2021-01-12", "2021-01-13", "2021-01-14"]}
        ref_data = {"val": [1, 1, 1, 2, 0, 1, 1]*2, "se": [np.nan] * 14,
                    "sample_size": [np.nan] * 14, "geo_id": ["1"] * 14,
                    "time_value": ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05", "2021-01-06", "2021-01-07",
                        "2021-01-08", "2021-01-09", "2021-01-10", "2021-01-11", "2021-01-12", "2021-01-13", "2021-01-14"]}

        test_df = pd.DataFrame(test_data)
        ref_df = pd.DataFrame(ref_data)
        validator.check_avg_val_vs_reference(
            test_df, ref_df,
            datetime.combine(date.today(), datetime.min.time()), "geo", "signal", report)

        assert len(report.raised_warnings) == 1
        assert report.raised_warnings[0].check_name == "check_test_vs_reference_avg_changed"

    def test_100x_val(self):
        validator = DynamicValidator(self.params)
        report = ValidationReport([])
        test_data = {"val": [1, 1, 1, 200, 0, 1, 1]*2, "se": [np.nan] * 14,
                     "sample_size": [np.nan] * 14, "geo_id": ["1"] * 14,
                     "time_value": ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05", "2021-01-06", "2021-01-07",
                        "2021-01-08", "2021-01-09", "2021-01-10", "2021-01-11", "2021-01-12", "2021-01-13", "2021-01-14"]}
        ref_data = {"val": [1, 1, 1, 2, 0, 1, 1]*2, "se": [np.nan] * 14,
                    "sample_size": [np.nan] * 14, "geo_id": ["1"] * 14,
                    "time_value": ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05", "2021-01-06", "2021-01-07",
                        "2021-01-08", "2021-01-09", "2021-01-10", "2021-01-11", "2021-01-12", "2021-01-13", "2021-01-14"]}

        test_df = pd.DataFrame(test_data)
        ref_df = pd.DataFrame(ref_data)
        validator.check_avg_val_vs_reference(
            test_df, ref_df,
            datetime.combine(date.today(), datetime.min.time()), "geo", "signal", report)

        assert len(report.raised_warnings) == 1
        assert report.raised_warnings[0].check_name == "check_test_vs_reference_avg_changed"

    def test_1000x_val(self):
        validator = DynamicValidator(self.params)
        report = ValidationReport([])
        test_data = {"val": [1, 1, 1, 2000, 0, 1, 1]*2, "se": [np.nan] * 14,
                     "sample_size": [np.nan] * 14, "geo_id": ["1"] * 14,
                     "time_value": ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05", "2021-01-06", "2021-01-07",
                        "2021-01-08", "2021-01-09", "2021-01-10", "2021-01-11", "2021-01-12", "2021-01-13", "2021-01-14"]}
        ref_data = {"val": [1, 1, 1, 2, 0, 1, 1]*2, "se": [np.nan] * 14,
                    "sample_size": [np.nan] * 14, "geo_id": ["1"] * 14,
                    "time_value": ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05", "2021-01-06", "2021-01-07",
                        "2021-01-08", "2021-01-09", "2021-01-10", "2021-01-11", "2021-01-12", "2021-01-13", "2021-01-14"]}

        test_df = pd.DataFrame(test_data)
        ref_df = pd.DataFrame(ref_data)
        validator.check_avg_val_vs_reference(
            test_df, ref_df,
            datetime.combine(date.today(), datetime.min.time()), "geo", "signal", report)

        assert len(report.raised_warnings) == 1
        assert report.raised_warnings[0].check_name == "check_test_vs_reference_avg_changed"


class TestDataOutlier:
    params = {
        "common": {
            "data_source": "",
            "span_length": 1,
            "end_date": "2020-09-02"
        }
    }
    pd.set_option("display.max_rows", None, "display.max_columns", None)
    # Test to determine outliers based on the row data, has lead and lag outlier

    def test_pos_outlier(self):
        validator = DynamicValidator(self.params)
        report = ValidationReport([])

        ref_val = [30, 30.28571429, 30.57142857, 30.85714286, 31.14285714,
                   31.42857143, 31.71428571, 32, 32, 32.14285714,
                   32.28571429, 32.42857143, 32.57142857, 32.71428571,
                   32.85714286, 33, 33, 33, 33, 33, 33, 33, 33,
                   33, 33, 33, 33.28571429, 33.57142857, 33.85714286, 34.14285714]
        test_val = [100, 100, 100]

        ref_data = {"val": ref_val, "se": [np.nan] * len(ref_val),
                    "sample_size": [np.nan] * len(ref_val), "geo_id": ["1"] * len(ref_val),
                    "time_value": pd.date_range(start="2020-09-24", end="2020-10-23")}
        test_data = {"val": test_val, "se": [np.nan] * len(test_val),
                     "sample_size": [np.nan] * len(test_val), "geo_id": ["1"] * len(test_val),
                     "time_value": pd.date_range(start="2020-10-24", end="2020-10-26")}

        ref_data2 = {"val": ref_val, "se": [np.nan] * len(ref_val),
                     "sample_size": [np.nan] * len(ref_val), "geo_id": ["2"] * len(ref_val),
                     "time_value": pd.date_range(start="2020-09-24", end="2020-10-23")}
        test_data2 = {"val": test_val, "se": [np.nan] * len(test_val),
                      "sample_size": [np.nan] * len(test_val), "geo_id": ["2"] * len(test_val),
                      "time_value": pd.date_range(start="2020-10-24", end="2020-10-26")}

        ref_df = pd.concat(
            [pd.DataFrame(ref_data), pd.DataFrame(ref_data2)]).reset_index(drop=True)
        test_df = pd.concat([pd.DataFrame(test_data), pd.DataFrame(test_data2)]). \
            reset_index(drop=True)

        validator.check_positive_negative_spikes(
            test_df, ref_df, "state", "signal", report)

        assert len(report.raised_warnings) == 2
        assert report.raised_warnings[0].check_name == "check_positive_negative_spikes"

    def test_neg_outlier(self):
        validator = DynamicValidator(self.params)
        report = ValidationReport([])

        ref_val = [100, 101, 100, 101, 100,
                   100, 100, 100, 100, 100,
                   100, 102, 100, 100, 100,
                   100, 100, 101, 100, 100,
                   100, 100, 100, 99, 100,
                   100, 98, 100, 100, 100]
        test_val = [10, 10, 10]

        ref_data = {"val": ref_val, "se": [np.nan] * len(ref_val),
                    "sample_size": [np.nan] * len(ref_val), "geo_id": ["1"] * len(ref_val),
                    "time_value": pd.date_range(start="2020-09-24", end="2020-10-23")}
        test_data = {"val": test_val, "se": [np.nan] * len(test_val),
                     "sample_size": [np.nan] * len(test_val), "geo_id": ["1"] * len(test_val),
                     "time_value": pd.date_range(start="2020-10-24", end="2020-10-26")}

        ref_data2 = {"val": ref_val, "se": [np.nan] * len(ref_val),
                     "sample_size": [np.nan] * len(ref_val), "geo_id": ["2"] * len(ref_val),
                     "time_value": pd.date_range(start="2020-09-24", end="2020-10-23")}
        test_data2 = {"val": test_val, "se": [np.nan] * len(test_val),
                      "sample_size": [np.nan] * len(test_val), "geo_id": ["2"] * len(test_val),
                      "time_value": pd.date_range(start="2020-10-24", end="2020-10-26")}

        ref_df = pd.concat([pd.DataFrame(ref_data), pd.DataFrame(ref_data2)]). \
            reset_index(drop=True)
        test_df = pd.concat([pd.DataFrame(test_data), pd.DataFrame(test_data2)]). \
            reset_index(drop=True)

        validator.check_positive_negative_spikes(
            test_df, ref_df, "state", "signal", report)

        assert len(report.raised_warnings) == 2
        assert report.raised_warnings[0].check_name == "check_positive_negative_spikes"

    def test_zero_outlier(self):
        validator = DynamicValidator(self.params)
        report = ValidationReport([])

        ref_val = [30, 30.28571429, 30.57142857, 30.85714286, 31.14285714,
                   31.42857143, 31.71428571, 32, 32, 32.14285714,
                   32.28571429, 32.42857143, 32.57142857, 32.71428571,
                   32.85714286, 33, 33, 33, 33, 33, 33, 33, 33,
                   33, 33, 33, 33.28571429, 33.57142857, 33.85714286, 34.14285714]
        test_val = [0, 0, 0]

        ref_data = {"val": ref_val, "se": [np.nan] * len(ref_val),
                    "sample_size": [np.nan] * len(ref_val), "geo_id": ["1"] * len(ref_val),
                    "time_value": pd.date_range(start="2020-09-24", end="2020-10-23")}
        test_data = {"val": test_val, "se": [np.nan] * len(test_val),
                     "sample_size": [np.nan] * len(test_val), "geo_id": ["1"] * len(test_val),
                     "time_value": pd.date_range(start="2020-10-24", end="2020-10-26")}

        ref_data2 = {"val": ref_val, "se": [np.nan] * len(ref_val),
                     "sample_size": [np.nan] * len(ref_val), "geo_id": ["2"] * len(ref_val),
                     "time_value": pd.date_range(start="2020-09-24", end="2020-10-23")}
        test_data2 = {"val": test_val, "se": [np.nan] * len(test_val),
                      "sample_size": [np.nan] * len(test_val), "geo_id": ["2"] * len(test_val),
                      "time_value": pd.date_range(start="2020-10-24", end="2020-10-26")}

        ref_df = pd.concat([pd.DataFrame(ref_data), pd.DataFrame(ref_data2)]). \
            reset_index(drop=True)
        test_df = pd.concat([pd.DataFrame(test_data), pd.DataFrame(test_data2)]). \
            reset_index(drop=True)

        validator.check_positive_negative_spikes(
            test_df, ref_df, "state", "signal", report)

        assert len(report.raised_warnings) == 1
        assert report.raised_warnings[0].check_name == "check_positive_negative_spikes"

    def test_no_outlier(self):
        validator = DynamicValidator(self.params)
        report = ValidationReport([])

        # Data from 51580 between 9/24 and 10/26 (10/25 query date)
        ref_val = [30, 30.28571429, 30.57142857, 30.85714286, 31.14285714,
                   31.42857143, 31.71428571, 32, 32, 32.14285714,
                   32.28571429, 32.42857143, 32.57142857, 32.71428571,
                   32.85714286, 33, 33, 33, 33, 33, 33, 33, 33,
                   33, 33, 33, 33, 33, 33, 33]
        test_val = [33, 33, 33]

        ref_data = {"val": ref_val, "se": [np.nan] * len(ref_val),
                    "sample_size": [np.nan] * len(ref_val), "geo_id": ["1"] * len(ref_val),
                    "time_value": pd.date_range(start="2020-09-24", end="2020-10-23")}
        test_data = {"val": test_val, "se": [np.nan] * len(test_val),
                     "sample_size": [np.nan] * len(test_val), "geo_id": ["1"] * len(test_val),
                     "time_value": pd.date_range(start="2020-10-24", end="2020-10-26")}

        ref_data2 = {"val": ref_val, "se": [np.nan] * len(ref_val),
                     "sample_size": [np.nan] * len(ref_val), "geo_id": ["2"] * len(ref_val),
                     "time_value": pd.date_range(start="2020-09-24", end="2020-10-23")}
        test_data2 = {"val": test_val, "se": [np.nan] * len(test_val),
                      "sample_size": [np.nan] * len(test_val), "geo_id": ["2"] * len(test_val),
                      "time_value": pd.date_range(start="2020-10-24", end="2020-10-26")}

        ref_df = pd.concat([pd.DataFrame(ref_data), pd.DataFrame(ref_data2)]). \
            reset_index(drop=True)
        test_df = pd.concat([pd.DataFrame(test_data), pd.DataFrame(test_data2)]). \
            reset_index(drop=True)

        validator.check_positive_negative_spikes(
            test_df, ref_df, "state", "signal", report)

        assert len(report.raised_warnings) == 0

    def test_source_api_overlap(self):
        validator = DynamicValidator(self.params)
        report = ValidationReport([])

        # Data from 51580 between 9/24 and 10/26 (10/25 query date)
        ref_val = [30, 30.28571429, 30.57142857, 30.85714286, 31.14285714,
                   31.42857143, 31.71428571, 32, 32, 32.14285714,
                   32.28571429, 32.42857143, 32.57142857, 32.71428571,
                   32.85714286, 33, 33, 33, 33, 33, 33, 33, 33, 33,
                   33, 33, 33, 33, 33, 33, 33, 33, 33]
        test_val = [100, 100, 100]

        ref_data = {"val": ref_val, "se": [np.nan] * len(ref_val),
                    "sample_size": [np.nan] * len(ref_val), "geo_id": ["1"] * len(ref_val),
                    "time_value": pd.date_range(start="2020-09-24", end="2020-10-26")}
        test_data = {"val": test_val, "se": [np.nan] * len(test_val),
                     "sample_size": [np.nan] * len(test_val), "geo_id": ["1"] * len(test_val),
                     "time_value": pd.date_range(start="2020-10-24", end="2020-10-26")}

        ref_data2 = {"val": ref_val, "se": [np.nan] * len(ref_val),
                     "sample_size": [np.nan] * len(ref_val), "geo_id": ["2"] * len(ref_val),
                     "time_value": pd.date_range(start="2020-09-24", end="2020-10-26")}
        test_data2 = {"val": test_val, "se": [np.nan] * len(test_val),
                      "sample_size": [np.nan] * len(test_val), "geo_id": ["2"] * len(test_val),
                      "time_value": pd.date_range(start="2020-10-24", end="2020-10-26")}

        ref_df = pd.concat([pd.DataFrame(ref_data), pd.DataFrame(ref_data2)]). \
            reset_index(drop=True)
        test_df = pd.concat([pd.DataFrame(test_data), pd.DataFrame(test_data2)]). \
            reset_index(drop=True)

        validator.check_positive_negative_spikes(
            test_df, ref_df, "state", "signal", report)

        assert len(report.raised_warnings) == 2
        assert report.raised_warnings[0].check_name == "check_positive_negative_spikes"
