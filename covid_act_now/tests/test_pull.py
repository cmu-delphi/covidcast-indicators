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
