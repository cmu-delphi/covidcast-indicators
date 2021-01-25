from os import listdir
from os.path import join

import pandas as pd
import pytest

from delphi_covid_act_now.constants import GEO_RESOLUTIONS

class TestRun:
    def test_output_files(self, run_as_module):
        csv_files = set(listdir("receiving"))

        expected_files = {
            f"20210101_{geo}_pcr_specimen_positivity_rate.csv"
            for geo in GEO_RESOLUTIONS}

        # All output files exist
        assert csv_files == expected_files

        # All output files have correct columns
        for csv_file in csv_files:
            df = pd.read_csv(join("receiving", csv_file))
            assert (df.columns.values == ["geo_id", "val", "se", "sample_size"]).all()
