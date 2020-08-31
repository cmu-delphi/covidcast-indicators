import pytest

from os.path import join, exists

import pandas as pd
from pandas.testing import assert_frame_equal
from delphi_google_health.run import run_module, add_prefix, public_signal
from delphi_google_health.constants import SIGNALS
from delphi_utils import read_params


class TestRunModule:

    def test_class(self, run_as_module, wip_signal=read_params()["wip_signal"]):
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

            assert_frame_equal(test_df, new_df, check_less_precise=5)

    def test_match_old_smoothed_output(self, run_as_module, wip_signal=read_params()["wip_signal"]):
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

            assert_frame_equal(test_df, new_df, check_less_precise=5)
