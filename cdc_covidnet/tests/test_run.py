from os.path import join, exists

import pandas as pd
from pandas.testing import assert_frame_equal

from delphi_cdc_covidnet.run import run_module

class TestRun:
    def test_match_old_to_new_output(self):
        output_fnames = ["202010_state_wip_covidnet.csv", "202011_state_wip_covidnet.csv"]

        # Run the whole program to completion
        run_module()

        for fname in output_fnames:
            # Target output file exist
            assert exists(join("receiving", fname))

            # Contents match
            expected_df = pd.read_csv(join("receiving_test", fname))
            actual_df = pd.read_csv(join("receiving", fname))
            assert_frame_equal(expected_df, actual_df, check_less_precise=5)
