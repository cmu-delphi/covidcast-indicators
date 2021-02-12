import pytest

from os import listdir
from os.path import join

import pandas as pd


class TestRun:
    def test_output_files_exist(self, run_as_module):

        csv_files = listdir("receiving")

        dates = [
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
        metrics = [
            "deaths_cumulative_num",
            "deaths_incidence_num",
            "deaths_incidence_prop",
            "confirmed_cumulative_num",
            "confirmed_incidence_num",
            "confirmed_incidence_prop",
            "deaths_7dav_cumulative_prop",
            "confirmed_7dav_cumulative_prop",
        ]

        expected_files = []
        for date in dates:
            for geo in geos:
                for metric in metrics:
                    expected_files += [date + "_" + geo + "_" + metric + ".csv"]

        set(csv_files) == set(expected_files)

    def test_output_file_format(self, run_as_module):

        df = pd.read_csv(
            join("receiving", "20200310_state_confirmed_cumulative_num.csv")
        )
        assert (df.columns.values == ["geo_id", "val", "se", "sample_size"]).all()
