"""Tests for running google_health."""

from os.path import join, exists

import pandas as pd
from pandas.testing import assert_frame_equal
from delphi_utils import read_params


class TestRunModule:
    """Tests for run_module()."""

    def test_class(self, run_as_module, wip_signal=read_params()["wip_signal"]):
        """Tests output file existence."""
        if wip_signal is True:
            assert exists(join("receiving", "20200419_hrr_wip_raw_search.csv"))
            assert exists(join("receiving", "20200419_msa_wip_raw_search.csv"))
            assert exists(join("receiving", "20200419_state_wip_raw_search.csv"))
            assert exists(join("receiving", "20200419_dma_wip_raw_search.csv"))

            assert exists(join("receiving", "20200315_hrr_wip_raw_search.csv"))
            assert exists(join("receiving", "20200315_msa_wip_raw_search.csv"))
            assert exists(join("receiving", "20200315_state_wip_raw_search.csv"))
            assert exists(join("receiving", "20200315_dma_wip_raw_search.csv"))
        else:
            assert exists(join("receiving", "20200419_hrr_raw_search.csv"))
            assert exists(join("receiving", "20200419_msa_raw_search.csv"))
            assert exists(join("receiving", "20200419_state_raw_search.csv"))
            assert exists(join("receiving", "20200419_dma_raw_search.csv"))

            assert exists(join("receiving", "20200315_hrr_raw_search.csv"))
            assert exists(join("receiving", "20200315_msa_raw_search.csv"))
            assert exists(join("receiving", "20200315_state_raw_search.csv"))
            assert exists(join("receiving", "20200315_dma_raw_search.csv"))

    def test_match_old_raw_output(self, run_as_module, wip_signal=read_params()["wip_signal"]):
        """Tests that raw output files don't change over time."""
        if wip_signal is True:
            files = [
                "20200419_hrr_wip_raw_search.csv",
                "20200419_msa_wip_raw_search.csv",
                "20200419_state_wip_raw_search.csv",
                "20200419_dma_wip_raw_search.csv",
            ]
        else:
            files = [
                "20200419_hrr_raw_search.csv",
                "20200419_msa_raw_search.csv",
                "20200419_state_raw_search.csv",
                "20200419_dma_raw_search.csv",
            ]

        for fname in files:
            test_df = pd.read_csv(join("receiving_test", fname))
            print(test_df)
            new_df = pd.read_csv(join("receiving", fname))
            print(new_df)

            assert_frame_equal(test_df, new_df)

    def test_match_old_smoothed_output(self, run_as_module, wip_signal=read_params()["wip_signal"]):
        """Tests that smooth output files don't change over time."""
        if wip_signal is True:

            files = [
                "20200419_hrr_wip_smoothed_search.csv",
                "20200419_msa_wip_smoothed_search.csv",
                "20200419_state_wip_smoothed_search.csv",
                "20200419_dma_wip_smoothed_search.csv",
            ]
        else:
            files = [
                "20200419_hrr_smoothed_search.csv",
                "20200419_msa_smoothed_search.csv",
                "20200419_state_smoothed_search.csv",
                "20200419_dma_smoothed_search.csv",
            ]

        for fname in files:
            test_df = pd.read_csv(join("receiving_test", fname))
            new_df = pd.read_csv(join("receiving", fname))

            assert_frame_equal(test_df, new_df)
