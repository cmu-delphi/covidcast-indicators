from os import listdir, remove
from os.path import join

import pandas as pd

from delphi_quidel_flutest.run import run_module


class TestRun:
    def test_output_files(self, run_as_module):
        
        # Test output exists
        csv_files = listdir("receiving")

        dates = [
            "20200623",
            "20200624",
            "20200625",
            "20200626",
            "20200627",
            "20200628",
            "20200629",
            "20200630",
            "20200701",
            "20200702",
            "20200703",
        ]
        geos = ["hrr", "msa", "state"]
        sensors = [
            "wip_flu_ag_raw_pct_positive",
            "wip_flu_ag_smoothed_pct_positive",
            "wip_flu_ag_raw_test_per_device",
            "wip_flu_ag_smoothed_test_per_device"
        ]

        expected_files = []
        for date in dates:
            for geo in geos:
                for sensor in sensors:
                    expected_files += [date + "_" + geo + "_" + sensor + ".csv"]

        assert set(expected_files).issubset(set(csv_files))
    
        # Test output format
        df = pd.read_csv(
            join("./receiving", "20200705_state_wip_flu_ag_raw_pct_positive.csv")
        )
        assert (df.columns.values == ["geo_id", "val", "se", "sample_size"]).all()
    
        # Test_intermediate_file
        flag = None
        for fname in listdir("./cache"):
            if ".csv" in fname:
                flag = 1
        assert flag is not None
        