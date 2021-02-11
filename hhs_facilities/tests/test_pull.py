"""Tests for running the geo conversion functions."""

from unittest.mock import patch

import pytest
import pandas as pd
import numpy as np

from delphi_hhs_facilities.pull import pull_data_iteratively, pull_data


class TestPull:

    @patch("delphi_hhs_facilities.pull.Epidata.covid_hosp_facility")
    @patch("delphi_hhs_facilities.pull.Epidata.covid_hosp_facility_lookup")
    def test_pull_data_iteratively(self, covid_hosp_facility_lookup, covid_hosp_facility):
        covid_hosp_facility_lookup.return_value = {
            "result": 1,
            "epidata": [{"hospital_pk": "020001"}, {"hospital_pk": "020006"}],
            "message": "success"
        }
        mock_epidata = [{"collection_week": 20201204,
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
                         "total_staffed_adult_icu_beds_7_day_avg": 12.1}]
        covid_hosp_facility.return_value = {
            "result": 1,
            "epidata": mock_epidata,
            "message": "success"}
        output = pull_data_iteratively({"state1", "state2"}, {"from": "test", "to": "date"})
        assert output == mock_epidata * 2  # x2 because there were 2 states that were looped through

        # test failure cases
        covid_hosp_facility.return_value = {"result": 2,
                                            "message": "test"}
        with pytest.raises(Exception) as exc:
            pull_data_iteratively({"state1", "state2"}, {"from": "test", "to": "date"})
        assert "Bad result from Epidata" in str(exc)
        covid_hosp_facility_lookup.return_value = {"result": 2, "message": "lookup fail"}
        with pytest.raises(Exception) as exc:
            pull_data_iteratively({"state1", "state2"}, {"from": "test", "to": "date"})
        assert "No results found" in str(exc)

    @patch("delphi_hhs_facilities.pull.pull_data_iteratively")
    def test_pull_data(self, pull_data_iteratively):
        pull_data_iteratively.return_value = [{"collection_week": 20201204,
                                               "total_beds_7_day_sum": 2360,
                                               "all_adult_hospital_beds_7_day_sum": -999999,
                                               "inpatient_beds_7_day_avg": -999999.0,
                                               "total_icu_beds_7_day_avg": np.nan,
                                               "total_staffed_adult_icu_beds_7_day_avg": 32.4}]
        output = pull_data()
        assert output.shape == (1, 7)  # 1 mock row, 6 mock + 1 new timestamp column
        # verify nans cast properly and timestamp added
        pd.testing.assert_frame_equal(
            output,
            pd.DataFrame({"collection_week": [20201204.],
                          "total_beds_7_day_sum": [2360.0],
                          "all_adult_hospital_beds_7_day_sum": [np.nan],
                          "inpatient_beds_7_day_avg": [np.nan],
                          "total_icu_beds_7_day_avg": [np.nan],
                          "total_staffed_adult_icu_beds_7_day_avg": [32.4],
                          "timestamp": [pd.Timestamp("2020-12-04")]}),
            check_names=False)
