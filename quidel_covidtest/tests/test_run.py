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
        assert (df.columns.values == ["geo_id", "val", "se", "sample_size"]).all()

        # 7/18/20 in test_data/test_data.csv has:
        # - zip 24526 -> fips 51019 with 21 positive counts and 3 negative counts
        # - zip 22079 -> fips 51059 with many positive and negative counts
        # - zip 24527 -> fips 51143 with 150 positive counts and 3 negative counts
        # I think that the following output file should then have
        # - fips 51059 for the many counts
        # - fips 51000 for the censored 24526 counts
        # - fips 51143 for the above-threshold 24527 counts
        # But instead, 51019 shows up with the same value as 51143. Is this right or am I missing something?
        df = pd.read_csv(join("./receiving", "20200718_county_covid_ag_smoothed_pct_positive.csv"))
        assert set(df.geo) == set(["51000", "51059", "51143"])

        # test_intermediate_file
        flag = None
        for fname in listdir("./cache"):
            if ".csv" in fname:
                flag = 1
        assert flag is not None
