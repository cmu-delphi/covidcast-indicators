from os.path import join, exists
from shutil import copytree

import pandas as pd
from pandas.testing import assert_frame_equal

from delphi_cdc_covidnet.run import run_module

class TestRun:
    def test_match_old_to_new_output(self):
        output_fnames = ["202010_state_wip_covidnet.csv", "202011_state_wip_covidnet.csv"]
        cached_files = [
            "networkid_2_catchmentid_11.json",
            "networkid_2_catchmentid_14.json",
            "networkid_2_catchmentid_17.json",
            "networkid_2_catchmentid_1.json",
            "networkid_2_catchmentid_20.json",
            "networkid_2_catchmentid_2.json",
            "networkid_2_catchmentid_3.json",
            "networkid_2_catchmentid_4.json",
            "networkid_2_catchmentid_7.json",
            "networkid_2_catchmentid_9.json",
            "networkid_3_catchmentid_15.json",
            "networkid_3_catchmentid_21.json",
            "networkid_3_catchmentid_5.json",
            "networkid_3_catchmentid_8.json",
        ]

        # Use these specific cached files to prevent unexpected backfill from actual new data
        copytree("cache_test", "cache", dirs_exist_ok=True)
        for cached_file in cached_files:
            assert exists(join("cache", cached_file)), f"Cached file '{cached_file}' not found"

        # Run the whole program to completion
        run_module()

        for fname in output_fnames:
            # Target output file exist
            assert exists(join("receiving", fname))

            # Contents match
            expected_df = pd.read_csv(join("receiving_test", fname))
            actual_df = pd.read_csv(join("receiving", fname))
            assert_frame_equal(expected_df, actual_df, check_less_precise=5)
