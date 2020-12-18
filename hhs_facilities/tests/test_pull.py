"""Tests for running the geo conversion functions."""

from unittest.mock import patch

import pytest
import pandas as pd
import numpy as np

from delphi_hhs_facilities.pull import pull_data


class TestPull:

    @patch("delphi_hhs_facilities.pull.Epidata.covid_hosp_facility")
    @patch("delphi_hhs_facilities.pull.Epidata.covid_hosp_facility_lookup")
    def test_pull(self, covid_hosp_facility_lookup, covid_hosp_facility):
        covid_hosp_facility_lookup.return_value = {
            "result": 1,
            "epidata": [{"hospital_pk": "020001"}, {"hospital_pk": "020006"}],
            "message": "success"
        }
        covid_hosp_facility.return_value = {
            "result": 1,
            "epidata": [{"collection_week": 20201204,
                         "total_beds_7_day_sum": 2360,
                         "all_adult_hospital_beds_7_day_sum": -999999,
                         "inpatient_beds_7_day_avg": -999999.0,
                         "total_icu_beds_7_day_avg": np.nan,
                         "total_staffed_adult_icu_beds_7_day_avg": 32.4},
                        {"collection_week": 20201204,
                         "total_beds_7_day_sum": 1000,
                         "all_adult_hospital_beds_7_day_sum": 1917,
                         "inpatient_beds_7_day_avg": 330.6,
                         "total_icu_beds_7_day_avg": 76.7,
                         "total_staffed_adult_icu_beds_7_day_avg": 12.1}],
            "message": "success"}
        output = pull_data()
        assert output.shape == (120, 7)  # 2 mock rows * 60 states, 6 mock + 1 new timestamp column
        # verify nans cast properly
        assert np.isnan(output["all_adult_hospital_beds_7_day_sum"][0])
        assert np.isnan(output["inpatient_beds_7_day_avg"][0])
        assert np.isnan(output["total_icu_beds_7_day_avg"][0])
        pd.testing.assert_series_equal(output.timestamp, 
                                       pd.Series([pd.Timestamp("2020-12-04")]*120),
                                       check_names=False)
        pd.testing.assert_series_equal(output.loc[0],
                                       pd.Series({"collection_week": 20201204,
                                                  "total_beds_7_day_sum": 2360,
                                                  "all_adult_hospital_beds_7_day_sum": np.nan,
                                                  "inpatient_beds_7_day_avg": np.nan,
                                                  "total_icu_beds_7_day_avg": np.nan,
                                                  "total_staffed_adult_icu_beds_7_day_avg": 32.4,
                                                  "timestamp": pd.Timestamp("2020-12-04")}), 
                                       check_names=False)

        # test failure cases
        covid_hosp_facility.return_value = {"result": 2, "message": "data fail"}
        with pytest.raises(Exception) as exc:
            pull_data()
            assert "data fail" in exc

        covid_hosp_facility_lookup.return_value = {"result": 2, "message": "lookup fail"}
        with pytest.raises(Exception) as exc:
            pull_data()
            assert "lookup fail" in exc
