import numpy as np
import pandas as pd
import pytest

from delphi_covid_act_now.pull import (
    load_data,
    extract_testing_metrics
)

class TestPull:
    def test_load_data(self, CAN_parquet_data, tmp_path):
        path = tmp_path / "small_CAN_data.parquet"
        CAN_parquet_data.to_parquet(path)

        df_pq = load_data(path)

        impt_cols = set([
            "fips", "timestamp",
            "age", "ethnicity", "sex",
            "location_type", "provider", "variable_name"
        ])

        assert impt_cols <= set(df_pq.columns)

    def test_zero_sample_size(self):
        columns = ["provider", "timestamp", "location_id", "fips", "location_type", "variable_name",
                "measurement", "unit", "age", "race", "ethnicity", "sex", "last_updated", "value"]
        df_pq = pd.DataFrame([
            # Should become a zero sample_size row
            ["cdc", "2021-01-01", "iso1:us#iso2:us-al#fips:01001", 1001, "county", "pcr_tests_positive",
                "rolling_average_7_day", "percentage", "all", "all", "all", "all", "2021-01-02 19:00:00", 0.0],
            ["cdc", "2021-01-01", "iso1:us#iso2:us-al#fips:01001", 1001, "county", "pcr_tests_total",
                "rolling_average_7_day", "specimens", "all", "all", "all", "all", "2021-01-02 19:00:00", 0.0],

            # A non-zero sample_size row
            ["cdc", "2021-01-01", "iso1:us#iso2:us-pa#fips:42001", 42001, "county", "pcr_tests_positive",
                "rolling_average_7_day", "percentage", "all", "all", "all", "all", "2021-01-02 19:00:00", 50.0],
            ["cdc", "2021-01-01", "iso1:us#iso2:us-pa#fips:42001", 42001, "county", "pcr_tests_total",
                "rolling_average_7_day", "specimens", "all", "all", "all", "all", "2021-01-02 19:00:00", 10.0],
        ], columns=columns)

        df_tests = extract_testing_metrics(df_pq)

        assert (df_tests.pcr_tests_total > 0).all()

    def test_extract_testing_data(self, CAN_parquet_data, tmp_path):
        path = tmp_path / "small_CAN_data.parquet"
        CAN_parquet_data.to_parquet(path)

        df_pq = load_data(path)
        df_tests = extract_testing_metrics(df_pq)

        impt_cols = set([
            "fips", "timestamp",
            "pcr_positivity_rate", "pcr_tests_positive", "pcr_tests_total",
        ])

        assert impt_cols <= set(df_tests.columns)
        assert df_tests["pcr_positivity_rate"].between(0, 1).all()
        assert np.allclose(
            df_tests.pcr_tests_positive,
            df_tests.pcr_positivity_rate * df_tests.pcr_tests_total)
