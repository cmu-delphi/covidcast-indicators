from os import listdir
from os.path import join, basename

import pandas as pd
import numpy as np
from delphi_jhu.run import add_nancodes
from delphi_utils import Nans


def _non_ignored_files_set(directory):
    """List all files in a directory not preceded by a '.' and store them in a set."""
    out = {fname for fname in listdir(directory) if not basename(fname).startswith(".")}
    return out

class TestRun:
    def test_output_files_exist(self, run_as_module):

        csv_files = _non_ignored_files_set("receiving")

        dates = [
            "20200303",
            "20200304",
            "20200305",
            "20200306",
            "20200307",
            "20200308",
            "20200309",
            "20200310"
        ]
        geos = ["county", "hrr", "msa", "state", "hhs", "nation"]
        signals = ["confirmed", "deaths"]
        metrics = [
            "cumulative_num",
            "cumulative_prop",
            "incidence_num",
            "incidence_prop",
            "7dav_incidence_num",
            "7dav_incidence_prop",
            "7dav_cumulative_num",
            "7dav_cumulative_prop",
        ]

        expected_files = {
            date + "_" + geo + "_" + signal + "_" + metric + ".csv"
            for date in dates
            for geo in geos
            for signal in signals
            for metric in metrics
        }

        assert csv_files == expected_files

    def test_output_file_format(self, run_as_module):

        df = pd.read_csv(
            join("receiving", "20200310_state_confirmed_cumulative_num.csv")
        )
        assert (
            df.columns.values
            == [
                "geo_id",
                "val",
                "se",
                "sample_size",
                "missing_val",
                "missing_se",
                "missing_sample_size",
            ]
        ).all()

    def test_add_nancodes(self):
        df = pd.DataFrame({
            "timestamp": pd.date_range("20200321", "20200328"),
            "geo_id": ["01017", "01043", "01061", "01103", "02282", "72001", "31000", "49000"],
            "val": [0.1, 0.2, 0.3, 0.4, 0.5, np.nan, 0.7, np.nan],
            "se": [np.nan] * 8,
            "sample_size": [np.nan] * 8
        }).set_index(["timestamp", "geo_id"])
        expected_df = pd.DataFrame({
            "timestamp": pd.date_range("20200321", "20200328"),
            "geo_id": ["01017", "01043", "01061", "01103", "02282", "72001", "31000", "49000"],
            "val": [0.1, 0.2, 0.3, 0.4, 0.5, np.nan, 0.7, np.nan],
            "se": [np.nan] * 8,
            "sample_size": [np.nan] * 8,
            "missing_val": [Nans.NOT_MISSING] * 5 + [Nans.REGION_EXCEPTION, Nans.NOT_MISSING, Nans.UNKNOWN],
            "missing_se": [Nans.NOT_APPLICABLE] * 8,
            "missing_sample_size": [Nans.NOT_APPLICABLE] * 8,
        }).set_index(["timestamp", "geo_id"])

        pd.testing.assert_frame_equal(add_nancodes(df, "deaths", "county", None), expected_df)

        df2 = pd.DataFrame({
            "timestamp": pd.date_range("20200321", "20200328"),
            "geo_id": ["01017", "01043", "01061", "01103", "02282", "72001", "31000", "49000"],
            "val": [np.nan] * 6 + [0.7, np.nan],
            "se": [np.nan] * 8,
            "sample_size": [np.nan] * 8
        }).set_index(["timestamp", "geo_id"])
        expected_df2 = pd.DataFrame({
            "timestamp": pd.date_range("20200321", "20200328"),
            "geo_id": ["01017", "01043", "01061", "01103", "02282", "72001", "31000", "49000"],
            "val": [np.nan] * 6 + [0.7, np.nan],
            "se": [np.nan] * 8,
            "sample_size": [np.nan] * 8,
            "missing_val": [Nans.PRIVACY] * 5 + [Nans.REGION_EXCEPTION, Nans.NOT_MISSING, Nans.UNKNOWN],
            "missing_se": [Nans.NOT_APPLICABLE] * 8,
            "missing_sample_size": [Nans.NOT_APPLICABLE] * 8,
        }).set_index(["timestamp", "geo_id"])

        pd.testing.assert_frame_equal(add_nancodes(df2, "deaths", "county", "seven_day_average"), expected_df2)
