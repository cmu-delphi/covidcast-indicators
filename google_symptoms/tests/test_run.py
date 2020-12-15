from os import listdir
from os.path import join
from itertools import product

import pandas as pd

class TestRun:
    def test_output_files_exist(self, run_as_module):

        csv_files = listdir("receiving")

        dates = [
            "20200801",
            "20200802",
            "20200803",
            "20200804",
            "20200805",
            "20200806",
            "20200807",
            "20200808",
            "20200809",
            "20200810",
            "20200811"
        ]
        geos = ["county", "state", "hhs", "nation"]
        metrics = ["anosmia", "ageusia", "sum_anosmia_ageusia"]
        smoother = ["raw", "smoothed"]

        expected_files = []
        for date, geo, metric, smoother in product(dates, geos, metrics, smoother):
            nf = "_".join([date, geo, metric, smoother, "research"]) + ".csv"
            expected_files.append(nf)

        set(csv_files) == set(expected_files)

    def test_output_file_format(self, run_as_module):

        df = pd.read_csv(
            join("receiving", "20200810_state_anosmia_smoothed_search.csv")
        )
        assert (df.columns.values == ["geo_id", "val", "se", "sample_size"]).all()
