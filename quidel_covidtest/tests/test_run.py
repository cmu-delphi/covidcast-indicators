"""Tests for running the quidel covidtest indicator."""
from itertools import product
import os
from os import listdir
from os.path import join
from pathlib import Path

import pandas as pd
import numpy as np

from delphi_utils import add_prefix
from delphi_quidel_covidtest.constants import PARENT_GEO_RESOLUTIONS, NONPARENT_GEO_RESOLUTIONS, \
    SENSORS, AGE_GROUPS

class TestRun:
    """Tests for run_module()."""

    def test_output_files(self, run_as_module, params):
        """Tests that the proper files are output."""

        # Test output exists
        csv_files = [i for i in listdir(params["common"]["export_dir"]) if i.endswith(".csv")]

        dates = [
            "20200718",
            "20200719",
            "20200720"
        ]
        geos = PARENT_GEO_RESOLUTIONS + NONPARENT_GEO_RESOLUTIONS
        sensors = add_prefix(SENSORS,
                             wip_signal=params["indicator"]["wip_signal"],
                             prefix="wip_")
        full_sensor = [f"{sensor}_{age}" for sensor, age in product(sensors, AGE_GROUPS)]

        expected_files = []
        for date in dates:
            for geo in PARENT_GEO_RESOLUTIONS:
                for sensor in sensors:
                    expected_files += [date + "_" + geo + "_" + sensor + ".csv"]
            for geo in NONPARENT_GEO_RESOLUTIONS:
                for sensor in full_sensor:
                    expected_files += [date + "_" + geo + "_" + sensor + ".csv"]

        assert set(expected_files) == (set(csv_files))
        assert '20200721_state_covid_ag_raw_pct_positive.csv' not in csv_files
        assert '20200722_state_covid_ag_raw_pct_positive.csv' not in csv_files

        # Test output format
        df = pd.read_csv(
            join("./receiving", "20200718_state_covid_ag_smoothed_pct_positive.csv")
        )
        assert (df.columns.values == ["geo_id", "val", "se", "sample_size"]).all()

        df = pd.read_csv(join("./receiving", "20200718_county_covid_ag_raw_pct_positive.csv"))
        #ZIP 24534, FIPS 51083 has 4 counts, 2 positives
        #ZIP 24529, FIPS 51117 has 24 counts, 12 positives
        #ZIP 24526, FIPS 51019 has 49 counts, 26 positives
        #MEGAFIPS 51000, should have 4+24+49 = 77 counts, 2+12+26 = 40 positives        
        #ZIP 24527, FIPS 51143 has 64 counts, 32 positives
        #ZIP 22079, FIPS 51059 has 60 counts, 24 positives
        assert set(df.geo_id) == set([51143, 51059, 51000])
        assert set(df.sample_size) == set([64, 60, 77])
        assert np.allclose(df.val.values, [(40+0.5)/(77+1)*100, (24+0.5)/(60+1)*100, 
                                           (32+0.5)/(64+1)*100], equal_nan=True)
        

        df = pd.read_csv(join("./receiving", "20200718_county_covid_ag_smoothed_pct_positive.csv"))
        assert set(df.geo_id) == set([51000, 51019, 51143, 51059])
        assert set(df.sample_size) == set([50, 50, 64, 60])
        parent_test = 4+24+49+64+60
        parent_pos = 2+12+26+32+24
        #ZIP 24526, FIPS 51019 has 49 counts, 26 positives, borrow 1 pseudo counts from VA
        #MEGAFIPS 51000, should have 4+24 = 28 counts, 2+12 = 14 positives, borrow 22 pseudo counts     
        #ZIP 24527, FIPS 51143 has 64 counts, 32 positives, do not borrow
        #ZIP 22079, FIPS 51059 has 60 counts, 24 positives, do not borrow
        assert np.allclose(df.val.values,
                           [(14+parent_pos*22/parent_test+0.5)/(50+1)*100, 
                            (26+parent_pos*1/parent_test+0.5)/(50+1)*100,
                            (24+0.5)/(60+1)*100,
                            (32+0.5)/(64+1)*100], equal_nan=True)

        # test_intermediate_file
        flag = None
        for fname in listdir("./cache"):
            if ".csv" in fname:
                flag = 1
        assert flag is not None

        for files in Path(params["common"]["export_dir"]).glob("*.csv"):
            os.remove(files)
        for files in Path(params["indicator"]["input_cache_dir"]).glob("*.csv"):
            os.remove(files)
