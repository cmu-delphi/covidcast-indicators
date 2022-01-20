"""Tests for running the quidel covidtest indicator."""
from os import listdir
from os.path import join

import pandas as pd

from delphi_utils import read_params, add_prefix
from delphi_quidel_covidtest.constants import PARENT_GEO_RESOLUTIONS, NONPARENT_GEO_RESOLUTIONS, \
    SENSORS
from delphi_quidel_covidtest.run import run_module


class TestRun:
    """Tests for run_module()."""

    PARAMS = {
        "common": {
            "export_dir": "./receiving"
        },
        "indicator": {
            "static_file_dir": "../static",
            "input_cache_dir": "./cache",
            "export_start_date": "2020-06-30",
            "export_end_date": "",
            "pull_start_date": "2020-07-09",
            "pull_end_date":"",
            "export_day_range":40,
            "aws_credentials": {
                "aws_access_key_id": "",
                "aws_secret_access_key": ""
            },
            "bucket_name": "",
            "wip_signal": "",
            "test_mode": True
        }
    }

    def test_output_files(self, clean_receiving_dir):
        """Tests that the proper files are output."""

        # Test output exists
        run_module(self.PARAMS)
        csv_files = [i for i in listdir("receiving") if i.endswith(".csv")]

        dates = [
            "20200718",
            "20200719",
            "20200720"
        ]
        geos = PARENT_GEO_RESOLUTIONS + NONPARENT_GEO_RESOLUTIONS
        sensors = add_prefix(SENSORS,
                             wip_signal=self.PARAMS["indicator"]["wip_signal"],
                             prefix="wip_")

        expected_files = []
        for date in dates:
            for geo in geos:
                for sensor in sensors:
                    expected_files += [date + "_" + geo + "_" + sensor + ".csv"]

        assert set(expected_files).issubset(set(csv_files))
        assert '20200721_state_covid_ag_raw_pct_positive.csv' not in csv_files
        assert '20200722_state_covid_ag_raw_pct_positive.csv' not in csv_files

        # Test output format
        df = pd.read_csv(
            join("./receiving", "20200718_state_covid_ag_smoothed_pct_positive.csv")
        )
        expected_columns = [
            "geo_id", "val", "se", "sample_size",
            "missing_val", "missing_se", "missing_sample_size"
        ]
        assert (df.columns.values == expected_columns).all()

        # test_intermediate_file
        flag = None
        for fname in listdir("./cache"):
            if ".csv" in fname:
                flag = 1
        assert flag is not None
