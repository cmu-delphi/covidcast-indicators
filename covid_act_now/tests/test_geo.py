
import numpy as np
import pandas as pd
import pytest

from delphi_covid_act_now.geo import (
    positivity_rate,
    std_err,
    geo_map
)

class TestAggregationFunctions:
    def test_pos_rate(self):
        df = pd.DataFrame({
            "pcr_tests_positive": [0, 1, 2, 3, 4, 5],
            "sample_size": [2, 2, 5, 10, 20, 50]
        })

        # The 0 sample_size case is expected to return 0 following the CDC's convention
        expected_pos_rate = [0, 0.5, 0.4, 0.3, 0.2, 0.1]
        pos_rate = positivity_rate(df)

        assert np.allclose(pos_rate, expected_pos_rate)

    def test_std_err(self):
        df = pd.DataFrame({
            "val": [0, 0.5, 0.4, 0.3, 0.2, 0.1],
            "sample_size": [2, 2, 5, 10, 20, 50]
        })

        expected_se = np.sqrt(df.val * (1 - df.val) / df.sample_size)
        se = std_err(df)

        # 0 se is permitted in this indicator, since applying the Jeffreys prior would violate the mirror
        assert (se >= 0).all()
        assert not np.isnan(se).any()
        assert not np.isinf(se).any()
        assert np.allclose(se, expected_se, equal_nan=True)

class TestGeoMap:
    def test_incorrect_geo(self, CAN_county_testing_data):
        df_county = CAN_county_testing_data

        with pytest.raises(ValueError):
            geo_map(df_county, "INVALID_GEO_RES")

    def test_incorrect_total(self):
        columns = ["fips", "timestamp", "pcr_tests_positive", "pcr_tests_total", "pcr_positivity_rate"]
        df_county = pd.DataFrame([
            ["01001", "2021-01-01", 20, 10, 2.0]
        ], columns=columns)

        with pytest.raises(ValueError):
            geo_map(df_county, "county")

    def test_zero_sample_size(self):
        columns = ["fips", "timestamp", "pcr_tests_positive", "pcr_tests_total", "pcr_positivity_rate"]
        df_county = pd.DataFrame([
            ["01001", "2021-01-01", 0, 0, 0]
        ], columns=columns)

        with pytest.raises(ValueError):
            geo_map(df_county, "county")

    def test_county(self, CAN_county_testing_data):
        df_county = CAN_county_testing_data
        df_new = geo_map(df_county, "county")

        assert np.allclose(df_new["val"], df_county["pcr_positivity_rate"])
        assert np.allclose(df_new["sample_size"], df_county["pcr_tests_total"])
        assert np.allclose(df_new["se"], df_county["se"], equal_nan=True)

    def test_state(self, CAN_county_testing_data, CAN_state_testing_data):
        df_county = CAN_county_testing_data
        df_state = CAN_state_testing_data
        df_new = geo_map(df_county, "state")

        assert np.allclose(df_new["val"], df_state["pcr_positivity_rate"])
        assert np.allclose(df_new["sample_size"], df_state["pcr_tests_total"])
        assert np.allclose(df_new["se"], df_state["se"], equal_nan=True)

    def test_msa(self, CAN_county_testing_data, CAN_msa_testing_data):
        df_county = CAN_county_testing_data
        df_msa = CAN_msa_testing_data
        df_new = geo_map(df_county, "msa")

        assert np.allclose(df_new["val"], df_msa["pcr_positivity_rate"])
        assert np.allclose(df_new["sample_size"], df_msa["pcr_tests_total"])
        assert np.allclose(df_new["se"], df_msa["se"], equal_nan=True)

    def test_hrr(self, CAN_county_testing_data, CAN_hrr_testing_data):
        df_county = CAN_county_testing_data
        df_hrr = CAN_hrr_testing_data
        df_new = geo_map(df_county, "hrr")

        assert np.allclose(df_new["val"], df_hrr["pcr_positivity_rate"])
        assert np.allclose(df_new["sample_size"], df_hrr["pcr_tests_total"])
        assert np.allclose(df_new["se"], df_hrr["se"], equal_nan=True)

    def test_hhs(self, CAN_county_testing_data, CAN_hhs_testing_data):
        df_county = CAN_county_testing_data
        df_hhs = CAN_hhs_testing_data
        df_new = geo_map(df_county, "hhs")

        assert np.allclose(df_new["val"], df_hhs["pcr_positivity_rate"])
        assert np.allclose(df_new["sample_size"], df_hhs["pcr_tests_total"])
        assert np.allclose(df_new["se"], df_hhs["se"], equal_nan=True)

    def test_nation(self, CAN_county_testing_data, CAN_nation_testing_data):
        df_county = CAN_county_testing_data
        df_nation = CAN_nation_testing_data
        df_new = geo_map(df_county, "nation")

        assert np.allclose(df_new["val"], df_nation["pcr_positivity_rate"])
        assert np.allclose(df_new["sample_size"], df_nation["pcr_tests_total"])
        assert np.allclose(df_new["se"], df_nation["se"], equal_nan=True)
