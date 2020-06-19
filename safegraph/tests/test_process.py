import pytest

from os import listdir
from os.path import join

import numpy as np
import pandas as pd
from delphi_safegraph.process import (
        construct_signals,
        aggregate,
    )
from delphi_safegraph.run import SIGNALS

signal_names, _ = (list(x) for x in zip(*SIGNALS))

class TestProcess:
    def test_construct_signals_present(self):

        cbg_df = construct_signals(pd.read_csv('raw_data/sample_raw_data.csv'),
                signal_names)
        assert 'completely_home_prop' in set(cbg_df.columns)
        assert 'full_time_work_prop' in set(cbg_df.columns)
        assert 'part_time_work_prop' in set(cbg_df.columns)
        assert 'median_home_dwell_time' in set(cbg_df.columns)

    def test_construct_signals_proportions(self):

        cbg_df = construct_signals(pd.read_csv('raw_data/sample_raw_data.csv'),
                signal_names)
        assert np.all(cbg_df['completely_home_prop'].values <= 1)
        assert np.all(cbg_df['full_time_work_prop'].values <= 1)
        assert np.all(cbg_df['part_time_work_prop'].values <= 1)

    def test_aggregate_county(self):
    
        cbg_df = construct_signals(pd.read_csv('raw_data/sample_raw_data.csv'),
                signal_names)
        df = aggregate(cbg_df, signal_names, 'county')

        assert np.all(df[f'{signal_names[0]}_n'].values > 0)
        x = df[f'{signal_names[0]}_se'].values
        assert np.all(x[~np.isnan(x)] >= 0)

    def test_aggregate_state(self):
    
        cbg_df = construct_signals(pd.read_csv('raw_data/sample_raw_data.csv'),
                signal_names)
        df = aggregate(cbg_df, signal_names, 'state')

        assert np.all(df[f'{signal_names[0]}_n'].values > 0)
        x = df[f'{signal_names[0]}_se'].values
        assert np.all(x[~np.isnan(x)] >= 0)

