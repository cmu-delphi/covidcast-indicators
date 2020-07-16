import pytest

from os import listdir
from os.path import join

import pandas as pd
from delphi_quidel_covidtest.run import run_module


class TestRun:
    def test_output_files_exist(self, run_as_module):

        csv_files = listdir("receiving")

        dates = [
            "20200610",
            "20200611",
            "20200612",
            "20200613",
            "20200614",
            "20200615",
            "20200616",
            "20200617",
            "20200618",
            "20200619",
            "20200620",
        ]
        geos = ["county", "hrr", "msa", "state"]
        sensors = [
            "raw_pct_positive",
            "smoothed_pct_positive"
        ]

        expected_files = []
        for date in dates:
            for geo in geos:
                for sensor in sensors:
                    expected_files += [date + "_" + geo + "_" + sensor + ".csv"]

        set(csv_files) == set(expected_files)

    def test_output_file_format(self, run_as_module):

        df = pd.read_csv(
            join("receiving", "20200610_state_raw_pct_positive.csv")
        )
        assert (df.columns.values == ["geo_id", "val", "se", "sample_size"]).all()
