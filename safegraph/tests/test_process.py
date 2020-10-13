"""Tests for Safegraph process functions."""
import os
import numpy as np
import pandas as pd
import pytest

from delphi_safegraph.process import (
    add_prefix,
    aggregate,
    construct_signals,
    files_in_past_week,
    process_window
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
            'x/y/z/2020/07/04/2020-07-04-social-distancing.csv.gz')) ==\
            ('x/y/z/2020/07/03/2020-07-03-social-distancing.csv.gz',
             'x/y/z/2020/07/02/2020-07-02-social-distancing.csv.gz',
             'x/y/z/2020/07/01/2020-07-01-social-distancing.csv.gz',
             'x/y/z/2020/06/30/2020-06-30-social-distancing.csv.gz',
             'x/y/z/2020/06/29/2020-06-29-social-distancing.csv.gz',
             'x/y/z/2020/06/28/2020-06-28-social-distancing.csv.gz')

    def test_process_window(self, tmp_path):
        export_dir = tmp_path / 'export'
        export_dir.mkdir()
        df1 = pd.DataFrame(data={
            'date_range_start': ['2020-06-12T00:00:00-05:00:00']*3,
            'origin_census_block_group': [10539707003, 10539707003, 10730144081],
            'device_count': [10, 20, 100],
            'completely_home_device_count': [20, 120, 400]
        })
        df2 = pd.DataFrame(data={
            'date_range_start': ['2020-06-11T00:00:00-05:00:00'],
            'origin_census_block_group': [10730144081],
            'device_count': [200],
            'completely_home_device_count': [4800]
        })
        process_window([df1, df2], ['completely_home_prop'], ['county'],
                       export_dir)
        expected = pd.DataFrame(data={
            'geo_id': [1053, 1073],
            'val': [4.0, 14.0],
            'se': [2.0, 10.0],
            'sample_size': [2, 2]
        })
        actual = pd.read_csv(
            export_dir / '2020-06-12_county_completely_home_prop.csv')
        print(expected)
        print(actual)
        assert set(expected.columns) == set(actual.columns)
        assert expected.equals(actual)
