"""Tests for the `run_module()` function."""
import os

import pandas as pd

from delphi_safegraph.constants import (SIGNALS,
                                        GEO_RESOLUTIONS)


class TestRun:
    """Tests for the `run_module()` function."""

    def test_output_files_exist(self, run_as_module):
        """Tests that the outputs of `run_module` exist."""
        csv_files = set(
            x for x in os.listdir("receiving") if x.endswith(".csv"))
        expected_files = set()
        for date in ("20200612", "20200611", "20200610"):
            for geo in GEO_RESOLUTIONS:
                for signal in SIGNALS:
                    print(date, geo, signal)
                    single_date_signal = "_".join([date, geo, signal]) + ".csv"
                    expected_files.add(single_date_signal)
                    single_date_signal = "_".join(
                        [date, geo, "wip", signal, "7dav"]) + ".csv"
                    expected_files.add(single_date_signal)

        assert expected_files == csv_files

    def test_output_files_format(self, run_as_module):
        """Tests that output files are in the correct format."""
        csv_files = os.listdir("receiving")
        for filename in csv_files:
            if not filename.endswith(".csv"):
                continue
            # Print the file name so that we can tell which file (if any)
            # triggered the error.
            print(filename)
            df = pd.read_csv(os.path.join("receiving", filename))
            assert (df.columns.values ==
                ["geo_id", "val", "se", "sample_size"]).all()
