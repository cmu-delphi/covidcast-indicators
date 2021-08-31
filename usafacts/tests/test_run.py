"""Tests for running the USAFacts indicator."""
import unittest
from itertools import product
from os import listdir, remove
from os.path import join
from unittest.mock import patch

import pandas as pd

from delphi_usafacts.run import run_module

def local_fetch(url, cache):
    return pd.read_csv(url)

def clean_dir(dir_path):
    """Remove csv files from a directory."""
    csv_files = [f for f in listdir(dir_path) if f.endswith(".csv")]
    for f in csv_files:
        remove(join(dir_path, f))

@patch("delphi_usafacts.pull.fetch", local_fetch)
class TestRun(unittest.TestCase):
    """Tests for the `run_module()` function."""
    PARAMS = {
        "common": {
            "export_dir": "./receiving",
            "input_dir": "./input_cache"
        },
        "indicator": {
            "base_url": "./test_data/small_{metric}.csv",
            "export_start_date": "2020-02-29"
        }
    }

    def test_run_module(self):
        """Test that run module produces reasonable files."""
        clean_dir(self.PARAMS["common"]["export_dir"])
        run_module(self.PARAMS)

        with self.subTest("Test that the expected output files exist."):
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
            geos = ["county", "hrr", "msa", "state", "hhs", "nation"]

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
                        if "7dav" in metric and "cumulative" in metric:
                            continue
                        expected_files += [date + "_" + geo + "_" + metric + ".csv"]
            assert set(csv_files) == set(expected_files)

        with self.subTest(" Test that the output files have the proper format."):
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
