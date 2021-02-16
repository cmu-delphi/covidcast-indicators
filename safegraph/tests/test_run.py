"""Tests for the `run_module()` function."""
import os

import pandas as pd

from delphi_safegraph.constants import (SIGNALS,
                                        GEO_RESOLUTIONS)
from delphi_safegraph.run import run_module

class TestRun:
    """Tests for the `run_module()` function."""

    PARAMS = {
        "common": {
            "export_dir": "./receiving"
        },
        "indicator": {
            "raw_data_dir": "./raw_data",
            "static_file_dir": "./static",
            "n_core": 12,
            "aws_access_key_id": "",
            "aws_secret_access_key": "",
            "aws_default_region": "",
            "aws_endpoint": "",
            "wip_signal" : ["median_home_dwell_time_7dav",
                            "completely_home_prop_7dav",
                            "part_time_work_prop_7dav",
                            "full_time_work_prop_7dav"],
            "sync": False
        }
    }

    def test_output_files_exist(self, clean_receiving_dir):
        """Tests that the outputs of `run_module` exist."""
        run_module(self.PARAMS)
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

    def test_output_files_format(self, clean_receiving_dir):
        """Tests that output files are in the correct format."""
        run_module(self.PARAMS)
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
