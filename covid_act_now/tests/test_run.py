from os import listdir
from os.path import join

import pandas as pd
import pytest

from delphi_covid_act_now.constants import GEO_RESOLUTIONS, SIGNALS
from delphi_covid_act_now.run import run_module

class TestRun:
    PARAMS = {
        "common": {
            "export_dir": "./receiving"
        },
        "indicator": {
            "parquet_url": "./test_data/small_CAN_data.parquet"
        }
    }

    def test_output_files(self, clean_receiving_dir):
        run_module(self.PARAMS)
        csv_files = set(listdir("receiving"))
        csv_files.discard(".gitignore")
        today = pd.Timestamp.today().date().strftime("%Y%m%d")

        expected_files = set()
        for signal in SIGNALS:
            for geo in GEO_RESOLUTIONS:
                expected_files.add(f"20210101_{geo}_{signal}.csv")

        # All output files exist
        assert csv_files == expected_files

        expected_columns = [
            "geo_id", "val", "se", "sample_size",
            "missing_val", "missing_se", "missing_sample_size"
        ]
        # All output files have correct columns
        for csv_file in csv_files:
            df = pd.read_csv(join("receiving", csv_file))
            assert (df.columns.values == expected_columns).all()
