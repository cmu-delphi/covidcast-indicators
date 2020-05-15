import pytest

from os.path import join, exists

import pandas as pd
from pandas.testing import assert_frame_equal
from delphi_google_health.run import run_module


class TestRunModule:
    def test_class(self, run_as_module):

        assert exists(join("receiving", "20200430_hrr_smoothed_search.csv"))
        assert exists(join("receiving", "20200430_hrr_raw_search.csv"))
        assert exists(join("receiving", "20200430_msa_raw_search.csv"))
        assert exists(join("receiving", "20200430_state_raw_search.csv"))
        assert exists(join("receiving", "20200430_dma_raw_search.csv"))

    def test_match_old_output(self, run_as_module):

        files = [
            "20200430_hrr_raw_search.csv",
            "20200430_msa_raw_search.csv",
            "20200430_state_raw_search.csv",
            "20200430_dma_raw_search.csv"
        ]

        for fname in files:
            test_df = pd.read_csv(join("receiving_test", fname))
            new_df = pd.read_csv(join("receiving", fname))

            assert_frame_equal(test_df, new_df)
