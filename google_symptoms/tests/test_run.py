import shutil
from datetime import datetime
from os import listdir
from os.path import join
from itertools import product

import pandas as pd

from conftest import TEST_DIR

class TestRun:
    @classmethod
    def teardown_class(cls):
        print('cleaning up tests...')
        shutil.rmtree(f"{TEST_DIR}/receiving/")

    def test_output_files_exist(self, run_as_module):
        output_files = listdir(f"{TEST_DIR}/receiving")
        smoothed_files = sorted(list(set([file for file in output_files if "smoothed" in file])))
        raw_files = sorted(list(set([file for file in output_files if "raw" in file])))
        csv_files = {"raw": raw_files, "smoothed": smoothed_files}

        expected_smoothed_dates = [
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
        expected_raw_dates = [
                                 '20200726',
                                 '20200727',
                                 '20200728',
                                 '20200729',
                                 '20200730',
                                 '20200731',
                             ] + expected_smoothed_dates

        dates = {
            "raw": expected_raw_dates,
            "smoothed": expected_smoothed_dates,
        }

        geos = ["county", "state", "hhs", "nation"]
        metrics = ["s01", "s02", "s03",
                   # "s04",
                   "s04", "s05",
                   # "s07",
                   "s06",
                   # "s09", "s10",
                   "scontrol"]
        smoother = ["raw", "smoothed"]

        for smther in smoother:
            expected_files = []

            for date, geo, metric in product(dates[smther], geos, metrics):
                nf = "_".join([date, geo, metric, smther, "search"]) + ".csv"
                expected_files.append(nf)

            csv_dates = list(set([datetime.strptime(f.split('_')[0], "%Y%m%d") for f in csv_files[smther] if smther in f]))
            assert set(csv_files[smther]).issuperset(set(expected_files))


    def test_output_file_format(self):
        df = pd.read_csv(
            join(f"{TEST_DIR}/receiving", "20200810_state_s03_smoothed_search.csv")
        )
        assert (df.columns.values == [
                "geo_id", "val", "se", "sample_size"]).all()
