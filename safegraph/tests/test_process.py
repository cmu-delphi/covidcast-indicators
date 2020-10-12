import pytest

from os import listdir
from os.path import join

import numpy as np
import pandas as pd
from delphi_safegraph.process import (
    construct_signals,
    aggregate,
    add_prefix,
    files_in_past_week
)
from delphi_safegraph.run import SIGNALS
from delphi_utils import read_params


class TestProcess:
    def test_construct_signals_present(self):
        cbg_df = construct_signals(pd.read_csv('raw_data/sample_raw_data.csv'),
                                   SIGNALS)
        assert 'completely_home_prop' in set(cbg_df.columns)
        assert 'full_time_work_prop' in set(cbg_df.columns)
        assert 'part_time_work_prop' in set(cbg_df.columns)
        assert 'median_home_dwell_time' in set(cbg_df.columns)

    def test_construct_signals_proportions(self):
        cbg_df = construct_signals(pd.read_csv('raw_data/sample_raw_data.csv'),
                                   SIGNALS)
        assert np.all(cbg_df['completely_home_prop'].values <= 1)
        assert np.all(cbg_df['full_time_work_prop'].values <= 1)
        assert np.all(cbg_df['part_time_work_prop'].values <= 1)

    def test_aggregate_county(self):
        cbg_df = construct_signals(pd.read_csv('raw_data/sample_raw_data.csv'),
                                   SIGNALS)
        df = aggregate(cbg_df, SIGNALS, 'county')

        assert np.all(df[f'{SIGNALS[0]}_n'].values > 0)
        x = df[f'{SIGNALS[0]}_se'].values
        assert np.all(x[~np.isnan(x)] >= 0)

    def test_aggregate_state(self):
        cbg_df = construct_signals(pd.read_csv('raw_data/sample_raw_data.csv'),
                                   SIGNALS)
        df = aggregate(cbg_df, SIGNALS, 'state')

        assert np.all(df[f'{SIGNALS[0]}_n'].values > 0)
        x = df[f'{SIGNALS[0]}_se'].values
        assert np.all(x[~np.isnan(x)] >= 0)

    def test_handle_wip_signal(self):
        # Test wip_signal = True
        signal_names = SIGNALS
        signal_names = add_prefix(SIGNALS, True, prefix="wip_")
        assert all(s.startswith("wip_") for s in signal_names)
        # Test wip_signal = list
        signal_names = add_prefix(SIGNALS, [SIGNALS[0]], prefix="wip_")
        assert signal_names[0].startswith("wip_")
        assert all(not s.startswith("wip_") for s in signal_names[1:])
        # Test wip_signal = False
        signal_names = add_prefix(["xyzzy", SIGNALS[0]], False, prefix="wip_")
        assert signal_names[0].startswith("wip_")
        assert all(not s.startswith("wip_") for s in signal_names[1:])

    def test_files_in_past_week(self):
        assert tuple(files_in_past_week(
            "data_dir/2020/07/04/2020-07-04-social-distancing.csv.gz")) ==\
            ("data_dir/2020/07/03/2020-07-03-social-distancing.csv.gz",
             "data_dir/2020/07/02/2020-07-02-social-distancing.csv.gz",
             "data_dir/2020/07/01/2020-07-01-social-distancing.csv.gz",
             "data_dir/2020/06/30/2020-06-30-social-distancing.csv.gz",
             "data_dir/2020/06/29/2020-06-29-social-distancing.csv.gz",
             "data_dir/2020/06/28/2020-06-28-social-distancing.csv.gz")
