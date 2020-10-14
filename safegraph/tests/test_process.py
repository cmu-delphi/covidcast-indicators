"""Tests for Safegraph process functions."""
import numpy as np
import pandas as pd

from delphi_safegraph.process import (
    add_prefix,
    aggregate,
    construct_signals,
    files_in_past_week,
    process,
    process_window
)
from delphi_safegraph.run import SIGNALS


class TestProcess:
    """Tests for processing Safegraph indicators."""

    def test_construct_signals_present(self):
        """Tests that all signals are constructed."""
        cbg_df = construct_signals(pd.read_csv('raw_data/sample_raw_data.csv'),
                                   SIGNALS)
        assert 'completely_home_prop' in set(cbg_df.columns)
        assert 'full_time_work_prop' in set(cbg_df.columns)
        assert 'part_time_work_prop' in set(cbg_df.columns)
        assert 'median_home_dwell_time' in set(cbg_df.columns)

    def test_construct_signals_proportions(self):
        """Tests that constructed signals are actual proportions."""
        cbg_df = construct_signals(pd.read_csv('raw_data/sample_raw_data.csv'),
                                   SIGNALS)
        assert np.all(cbg_df['completely_home_prop'].values <= 1)
        assert np.all(cbg_df['full_time_work_prop'].values <= 1)
        assert np.all(cbg_df['part_time_work_prop'].values <= 1)

    def test_aggregate_county(self):
        """Tests that aggregation at the county level creates non-zero-valued
        signals."""
        cbg_df = construct_signals(pd.read_csv('raw_data/sample_raw_data.csv'),
                                   SIGNALS)
        df = aggregate(cbg_df, SIGNALS, 'county')

        assert np.all(df[f'{SIGNALS[0]}_n'].values > 0)
        x = df[f'{SIGNALS[0]}_se'].values
        assert np.all(x[~np.isnan(x)] >= 0)

    def test_aggregate_state(self):
        """Tests that aggregation at the state level creates non-zero-valued
        signals."""
        cbg_df = construct_signals(pd.read_csv('raw_data/sample_raw_data.csv'),
                                   SIGNALS)
        df = aggregate(cbg_df, SIGNALS, 'state')

        assert np.all(df[f'{SIGNALS[0]}_n'].values > 0)
        x = df[f'{SIGNALS[0]}_se'].values
        assert np.all(x[~np.isnan(x)] >= 0)

    def test_handle_wip_signal(self):
        """Tests that `add_prefix()` derives work-in-progress signals."""
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
        """Tests that `files_in_past_week()` finds the file names corresponding
        to the previous 6 days."""
        # Week that stretches over a month boundary.
        assert tuple(files_in_past_week(
            'x/y/z/2020/07/04/2020-07-04-social-distancing.csv.gz')) ==\
            ('x/y/z/2020/07/03/2020-07-03-social-distancing.csv.gz',
             'x/y/z/2020/07/02/2020-07-02-social-distancing.csv.gz',
             'x/y/z/2020/07/01/2020-07-01-social-distancing.csv.gz',
             'x/y/z/2020/06/30/2020-06-30-social-distancing.csv.gz',
             'x/y/z/2020/06/29/2020-06-29-social-distancing.csv.gz',
             'x/y/z/2020/06/28/2020-06-28-social-distancing.csv.gz')
        # Week that stretches over a year boundary.
        assert tuple(files_in_past_week(
            'x/y/z/2020/01/04/2020-01-04-social-distancing.csv.gz')) ==\
            ('x/y/z/2020/01/03/2020-01-03-social-distancing.csv.gz',
             'x/y/z/2020/01/02/2020-01-02-social-distancing.csv.gz',
             'x/y/z/2020/01/01/2020-01-01-social-distancing.csv.gz',
             'x/y/z/2019/12/31/2019-12-31-social-distancing.csv.gz',
             'x/y/z/2019/12/30/2019-12-30-social-distancing.csv.gz',
             'x/y/z/2019/12/29/2019-12-29-social-distancing.csv.gz')
        # Week that includes a leap day.
        assert tuple(files_in_past_week(
            'x/y/z/2020/03/01/2020-03-01-social-distancing.csv.gz')) ==\
            ('x/y/z/2020/02/29/2020-02-29-social-distancing.csv.gz',
             'x/y/z/2020/02/28/2020-02-28-social-distancing.csv.gz',
             'x/y/z/2020/02/27/2020-02-27-social-distancing.csv.gz',
             'x/y/z/2020/02/26/2020-02-26-social-distancing.csv.gz',
             'x/y/z/2020/02/25/2020-02-25-social-distancing.csv.gz',
             'x/y/z/2020/02/24/2020-02-24-social-distancing.csv.gz')

    def test_process_window(self, tmp_path):
        """Tests that processing over a window correctly aggregates signals."""
        export_dir = tmp_path / 'export'
        export_dir.mkdir()
        df1 = pd.DataFrame(data={
            'date_range_start': ['2020-02-14T00:00:00-05:00:00']*3,
            'origin_census_block_group': [10539707003,
                                          10539707003,
                                          10730144081],
            'device_count': [100, 200, 1000],
            'completely_home_device_count': [2, 12, 40]
        })
        df2 = pd.DataFrame(data={
            'date_range_start': ['2020-02-14T00:00:00-05:00:00'],
            'origin_census_block_group': [10730144081],
            'device_count': [2000],
            'completely_home_device_count': [480]
        })
        process_window([df1, df2], ['completely_home_prop'], ['county'],
                       export_dir)
        expected = pd.DataFrame(data={
            'geo_id': [1053, 1073],
            'val': [0.04, 0.14],
            'se': [0.02, 0.10],
            'sample_size': [2, 2]
        })
        actual = pd.read_csv(
            export_dir / '2020-02-14_county_completely_home_prop.csv')
        print(expected)
        print(actual)
        pd.testing.assert_frame_equal(expected, actual)

    def test_process(self, tmp_path):
        """Tests that processing a list of current and previous file names
        correctly reads and aggregates signals."""
        export_dir = tmp_path / 'export'
        export_dir.mkdir()

        process('raw_data/small_raw_data_0.csv',
                # File 2 does not exist.
                ['raw_data/small_raw_data_1.csv',
                 'raw_data/small_raw_data_2.csv',
                 'raw_data/small_raw_data_3.csv', ],
                SIGNALS,
                ['state'],
                export_dir)

        expected = {
            'median_home_dwell_time': pd.DataFrame(data={
                'geo_id': ['al', 'pa'],
                'val': [4.5, 7.5],
                'se': [1.5, 0.5],
                'sample_size': [2, 2]
            }),
            'completely_home_prop': pd.DataFrame(data={
                'geo_id': ['al', 'pa'],
                'val': [0.1, 0.15],
                'se': [0.05, 0.05],
                'sample_size': [2, 2]
            }),
            'part_time_work_prop': pd.DataFrame(data={
                'geo_id': ['al', 'pa'],
                'val': [0.25, 0.25],
                'se': [0.1, 0.05],
                'sample_size': [2, 2]
            }),
            'full_time_work_prop': pd.DataFrame(data={
                'geo_id': ['al', 'pa'],
                'val': [0.35, 0.35],
                'se': [0.1, 0.05],
                'sample_size': [2, 2]
            })
        }
        actual = {signal: pd.read_csv(
            export_dir / f'2020-06-12_state_{signal}.csv')
            for signal in SIGNALS}
        for signal in SIGNALS:
            pd.testing.assert_frame_equal(expected[signal], actual[signal])
