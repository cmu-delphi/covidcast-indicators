import pytest

from os import listdir
from os.path import join

import pandas as pd
from delphi_cds_testing.run import run_module


class TestRun:
    def test_output_files_exist(self, run_as_module):

        csv_files = listdir("receiving")

        dates = [
            "20200603",
            "20200604",
            "20200605",
            "20200606",
            "20200607",
            "20200608",
            "20200609",
            "20200610",
        ]
        geos = ["county", "hrr", "msa", "state"]
        metrics = [
            "cumulative_num",
            "incidence_num",
            "incidence_prop",
            "cumulative_num",
            "incidence_num",
            "incidence_prop",
            "wip_7dav_cumulative_prop",
            "wip_7dav_cumulative_prop",
        ]

        expected_files = []
        for date in dates:
            for geo in geos:
                for metric in metrics:
                    expected_files += [date + "_" + geo + "_" + metric + ".csv"]

        set(csv_files) == set(expected_files)

    def test_output_file_format(self, run_as_module):

        df = pd.read_csv(
            join("receiving", "20200610_state_cumulative_num.csv")
        )
        assert (df.columns.values == ["geo_id", "val", "se", "sample_size"]).all()
