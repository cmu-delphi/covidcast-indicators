import pytest

from os import listdir
from os.path import join

import pandas as pd
from delphi_cds.run import run_module


class TestRun:
    def test_output_files_exist(self, run_as_module):

        csv_files = listdir("receiving")

        dates = [
            "20200704",
            "20200705",
            "20200706",
            "20200707",
            "20200708",
            "20200709",
            "20200710",
        ]
        geos = ["county", "hrr", "msa", "state"]
        metrics = [
            "cumulative_num",
            "incidence_num",
            "incidence_prop",
            "cumulative_prop",
            "7dav_cumulative_prop",
            "7dav_cumulative_num",
            "7dav_incidence_num",
            "7dav_incidence_prop",
        ]

        expected_files = []
        for date in dates:
            for geo in geos:
                for metric in metrics:
                    expected_files += [date + "_" + geo + "_wip_" + metric + ".csv"]

        set(csv_files) == set(expected_files)

    def test_output_file_format(self, run_as_module):

        df = pd.read_csv(
            join("receiving", "20200710_state_wip_tested_cumulative_num.csv")
        )
        assert (df.columns.values == ["geo_id", "val", "se", "sample_size"]).all()
