from os import listdir
from os.path import join, basename

import pandas as pd


class TestRun:
    def test_output_files_exist(self, run_as_module):

        csv_files = [x for x in listdir("receiving") if not basename(x).startswith(".")]

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
        geos = ["county", "hrr", "msa", "state", "hhs", "nation"]
        metrics = []
        for event in ["confirmed", "deaths"]:
            for smoothing in ["", "_7dav"]:
                for window in ["incidence", "cumulative"]:
                    for stat in ["num", "prop"]:
                        metrics.append(f"{event}{smoothing}_{window}_{stat}")

        expected_files = []
        for date in dates:
            for geo in geos:
                for metric in metrics:
                    # Can't compute 7dav for first few days of data because of NAs
                    if date > "20200305" or "7dav" not in metric:
                        expected_files += [date + "_" + geo + "_" + metric + ".csv"]

        assert set(csv_files) == set(expected_files)

    def test_output_file_format(self, run_as_module):

        df = pd.read_csv(
            join("receiving", "20200310_state_confirmed_cumulative_num.csv")
        )
        assert (df.columns.values == ["geo_id", "val", "se", "sample_size"]).all()
