# """Tests for running files."""

import pandas as pd
from pandas.testing import assert_frame_equal
import os
from delphi_chng_flags.run import (
    run_module)
import pytest
import shutil

def test_output_files():

    PARAMS = {
          "common": {
            "export_dir": "./receiving",
            "log_filename": "./receiving/logger.log"
          },
          "indicator": {
            "input_cache_dir": "./cache/test_cache_with_file",
            "num_lags":2,
            "n_train":4,
            "n_test":2,
            "n_valid":1,
            "lags": [1, 60],
            "start_date": "01/10/2022",
            "end_date": "01/24/2022"
          }
        }
#     """Tests for running the module."""
    """Tests that the output files contain the correct results of the run."""
    run_module(PARAMS)
    cache = "./cache/test_cache_with_file"
    assert_frame_equal(pd.read_csv(cache + '/resid_4_2.csv', header=0, parse_dates=['date']),
                       pd.read_csv(cache + '/resid_4_2_orig.csv', header=0, parse_dates=['date']),
                       check_dtype=False)



    assert_frame_equal(pd.read_csv(cache + '/flag_ar_4_2.csv', header=0, parse_dates=['date']),
                       pd.read_csv(cache + '/flag_ar_4_2_orig.csv', header=0, parse_dates=['date']),
    check_dtype = False)

    assert_frame_equal(pd.read_csv(cache + '/flag_spike_4_2.csv', header=0, parse_dates=['date']),
                       pd.read_csv(cache + '/flag_spike_4_2_orig.csv', header=0, parse_dates=['date']),
                       check_dtype=False)




#TODO: Find an example of this
# def cvxpy_fail():
#     return
#
