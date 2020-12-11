"""Tests for running the signal generation functions."""

import pandas as pd
import numpy as np

from delphi_hhs_facilities.generate_signals import generate_signal, sum_cols


class TestGenerateSignals:

    def test_generate_signals(self):
        test_input = pd.DataFrame(
            {"a": [1, 2, 3, 4, np.nan],
             "b": [2, np.nan, 4, 6, np.nan],
             "geo_id": ["x", "x", "x", "y", "z"],
             "timestamp": [pd.Timestamp("20200201"),
                           pd.Timestamp("20200201"),
                           pd.Timestamp("20200202"),
                           pd.Timestamp("20200203"),
                           pd.Timestamp("20200204")]
             })
        test_output = generate_signal(test_input, ["a", "b"], sum_cols, -1)
        expected = pd.DataFrame(
            {"timestamp": [pd.Timestamp("20200131"),
                           pd.Timestamp("20200201"),
                           pd.Timestamp("20200202")],
             "geo_id": ["x", "x", "y"],
             "val": [5., 7., 10.],
             "se": [np.nan]*3,
             "sample_size": [np.nan]*3
             })
        pd.testing.assert_frame_equal(test_output, expected)

    def test_sum_cols(self):
        test_input = [
            pd.Series([1, 2, 3, np.nan, np.nan]),
            pd.Series([np.nan, 3, 6, 9, np.nan])
            ]
        test_output = sum_cols(test_input)
        expected = pd.Series([1, 5, 9, 9, np.nan])
        pd.testing.assert_series_equal(test_output, expected)
