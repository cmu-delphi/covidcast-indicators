from os import listdir, remove
from os.path import join

import pandas as pd

from delphi_quidel_covidtest.run import run_module


class TestRun:
    def test_output_files(self, run_as_module):
        
        # Test output exists
        csv_files = listdir("receiving")

        dates = [
            "20200610",
            "20200611",
            "20200612",
            "20200613",
            "20200614",
            "20200615",
            "20200616",
            "20200617",
            "20200618",
            "20200619",
            "20200620",
        ]
        geos = ["county", "hrr", "msa", "state"]
        sensors = [
            "wip_flu_ag_raw_pct_positive",
            "wip_flu_ag_smoothed_pct_positive"
            "wip_flu_ag_test_per_device",
            "wip_flu_ag_test_per_device"
        ]

        expected_files = []
        for date in dates:
            for geo in geos:
                for sensor in sensors:
                    expected_files += [date + "_" + geo + "_" + sensor + ".csv"]

        assert set(expected_files).issubset(set(csv_files))
        
        # Test output format
        df = pd.read_csv(
            join("./receiving", "20200610_state_wip_flu_ag_raw_pct_positive.csv")
        )
        assert (df.columns.values == ["geo_id", "val", "se", "sample_size"]).all()
        
        # test_intermediate_file
        flag = None
        for fname in listdir("./cache"):
            if ".csv" in fname:
                remove(join("cache", fname))
                flag = 1
        assert flag is not None
        