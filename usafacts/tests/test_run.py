"""Tests for running the USAFacts indicator."""
from itertools import product
from os import listdir
from os.path import join

import pandas as pd


class TestRun:
    """Tests for the `run_module()` function."""
    def test_output_files_exist(self, run_as_module):
        """Test that the expected output files exist."""
        csv_files = [f for f in listdir("receiving") if f.endswith(".csv")]

        dates = [
            "20200229",
            "20200301",
            "20200302",
            "20200303",
            "20200304",
            "20200305",
            "20200306",
            "20200307",
            "20200308",
            "20200309",
            "20200310",
        ]
        geos = ["county", "hrr", "msa", "state"]

        # enumerate metric names.
        metrics = []
        for event, span, stat in product(["deaths", "confirmed"],
                                         ["cumulative", "incidence"],
                                         ["num", "prop"]):
            metrics.append("_".join([event, span, stat]))
            metrics.append("_".join([event, "7dav", span, stat]))

        expected_files = []
        for date in dates:
            for geo in geos:
                for metric in metrics:
                    if "7dav" in metric and date in dates[:6]:
                        continue  # there are no 7dav signals for first 6 days
                    expected_files += [date + "_" + geo + "_" + metric + ".csv"]

        assert set(csv_files) == set(expected_files)

    def test_output_file_format(self, run_as_module):
        """Test that the output files have the proper format."""
        df = pd.read_csv(
            join("receiving", "20200310_state_confirmed_cumulative_num.csv")
        )
        assert (df.columns.values == ["geo_id", "val", "se", "sample_size"]).all()
