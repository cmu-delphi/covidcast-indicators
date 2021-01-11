"""Tests for running the quidel covidtest indicator."""
from os import listdir
from os.path import join

import pandas as pd

from delphi_utils import read_params, add_prefix
from delphi_quidel_covidtest.constants import PARENT_GEO_RESOLUTIONS, NONPARENT_GEO_RESOLUTIONS, \
    SENSORS


class TestRun:
    """Tests for run_module()."""
    def test_output_files(self, run_as_module):
        """Tests that the proper files are output."""

        # Test output exists
        csv_files = [i for i in listdir("receiving") if i.endswith(".csv")]

        dates = [
            "20200702",
            "20200703",
            "20200704",
            "20200705",
            "20200706",
            "20200707",
            "20200708",
            "20200709"
        ]
        geos = PARENT_GEO_RESOLUTIONS + NONPARENT_GEO_RESOLUTIONS
        sensors = add_prefix(SENSORS,
                             wip_signal=read_params()["wip_signal"],
                             prefix="wip_")

        expected_files = []
        for date in dates:
            for geo in geos:
                for sensor in sensors:
                    expected_files += [date + "_" + geo + "_" + sensor + ".csv"]

        assert set(expected_files) == (set(csv_files))

        # Test output format
        df = pd.read_csv(
            join("./receiving", "20200709_state_covid_ag_raw_pct_positive.csv")
        )
        assert (df.columns.values == ["geo_id", "val", "se", "sample_size"]).all()

        # test_intermediate_file
        flag = None
        for fname in listdir("./cache"):
            if ".csv" in fname:
                flag = 1
        assert flag is not None
