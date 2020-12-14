"""Tests for running the geo conversion functions."""
from unittest.mock import patch
import tempfile
import os

import pandas as pd
import numpy as np

from delphi_hhs_facilities.run import run_module


class TestRun:

    @patch("delphi_hhs_facilities.run.read_params")
    @patch("delphi_hhs_facilities.run.pull_data")
    def test_run_module(self, pull_data, params):
        pull_data.return_value = pd.DataFrame({
            "timestamp": [pd.Timestamp("20200201"),
                          pd.Timestamp("20200201"),
                          pd.Timestamp("20200201"),
                          pd.Timestamp("20200201")],
            "fips_code": ["25013", "25013", np.nan, np.nan],
            "zip": ["01001", "01001", "00601", "00601"],
            "state": ["AL", "AL", "PR", np.nan],
            "previous_day_admission_adult_covid_confirmed_7_day_sum": [1., 2., 3., 4.],
            "previous_day_admission_pediatric_covid_confirmed_7_day_sum": [10., 20., 30., 40.],
            "previous_day_admission_adult_covid_suspected_7_day_sum": [0, 50, 0, 50],
            "previous_day_admission_pediatric_covid_suspected_7_day_sum": [5., 10., 15., 20.]
        })
        # pd.testing.assert_frame_equal(
        #     pd.read_csv(os.path.join(tmp, "20200131_county_confirmed_admissions_7d.csv")),
        #     pd.DataFrame({"geo_id": [25013, 72001, 72141],
        #                   "val": [33.0, 76.56462035541196, 0.4353796445880453],
        #                   "se": [np.nan] * 3,
        #                   "sample_size": [np.nan] * 3})
        # )
        with tempfile.TemporaryDirectory() as tmp:
            params.return_value = {"export_dir": tmp}
            run_module()
            expected_files = ["20200131_county_confirmed_admissions_7d.csv",
                              "20200131_county_confirmed_suspected_admission_7d.csv",
                              "20200131_hrr_confirmed_admissions_7d.csv",
                              "20200131_hrr_confirmed_suspected_admission_7d.csv",
                              "20200131_msa_confirmed_admissions_7d.csv",
                              "20200131_msa_confirmed_suspected_admission_7d.csv",
                              "20200131_state_confirmed_admissions_7d.csv",
                              "20200131_state_confirmed_suspected_admission_7d.csv"]
            assert sorted(os.listdir(tmp)) == sorted(expected_files)
            for f in expected_files:
                out_df = pd.read_csv(os.path.join(tmp, f))
                expected_df = pd.read_csv(os.path.join("expected", f))
                pd.testing.assert_frame_equal(out_df, expected_df)
