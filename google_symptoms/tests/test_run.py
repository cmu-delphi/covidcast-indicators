import logging
from os import listdir
from os.path import join
from itertools import product

import pandas as pd
import numpy as np
import pytest
import freezegun

from conftest import TEST_DIR
from delphi_google_symptoms.constants import COMBINED_METRIC, GEO_RESOLUTIONS
class TestRun:
    @pytest.mark.freeze_time("2020-08-15")
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
            "20200811",
            "20200812",
            "20200813",
            "20200814",
            "20200815"
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

        geos = ["county", "state", "hhs", "hrr", "nation", "msa"]
        metrics = ["s01", "s02", "s03",
                   "s04", "s05",
                   "s06", "s07",
                   "scontrol"]
        smoother = ["raw", "smoothed"]

        for smther in smoother:
            expected_files = []

            for date, geo, metric in product(dates[smther], geos, metrics):
                nf = "_".join([date, geo, metric, smther, "search"]) + ".csv"
                expected_files.append(nf)
                file_prefix = f"{geo}_{metric}_{smther}"
                files_with_prefix = [f for f in output_files if file_prefix in f]
                if smoother == "raw":
                    assert len(files_with_prefix) == 21
                if smoother == "smoothed":
                    assert len(files_with_prefix) == 15
            logging.info(csv_files[smther])
            assert set(csv_files[smther]) == (set(expected_files))

    @pytest.mark.freeze_time("2020-08-20")
    def test_output_file_format(self, run_as_module):
        df = pd.read_csv(
            join(f"{TEST_DIR}/receiving", "20200810_state_s03_smoothed_search.csv")
        )
        assert (df.columns.values == [
                "geo_id", "val", "se", "sample_size"]).all()

    @pytest.mark.freeze_time("2020-08-20")
    def test_output_files_smoothed(self, run_as_module):
        dates = [str(x) for x in range(20200804, 20200811)]

        smoothed = pd.read_csv(
            join(f"{TEST_DIR}/receiving",
                 f"{dates[-1]}_state_s01_smoothed_search.csv")
        )

        raw = pd.concat([
            pd.read_csv(
                join(f"{TEST_DIR}/receiving",
                     f"{date}_state_s01_raw_search.csv")
            ) for date in dates
        ])

        raw = raw.groupby('geo_id')['val'].sum()/7.0
        df = pd.merge(smoothed, raw, on='geo_id',
                      suffixes=('_smoothed', '_raw'))

        assert np.allclose(df['val_smoothed'].values, df['val_raw'].values)
