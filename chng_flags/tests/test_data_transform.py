"""Tests for methods in data_transform.py"""
import pandas as pd
from pandas.testing import assert_frame_equal
from delphi_chng_flags.data_transform import (
    identify_correct_spikes,
    weekend_corr,
    ar_method)

from delphi_utils import (
    get_structured_logger,
    Weekday
)

def test_identify_correct_spikes():
    cache = "./cache/test_data_transforms"
    df = pd.read_csv(f'{cache}/df.csv', index_col=0, parse_dates=[0])
    spikes_df, flags = identify_correct_spikes(df)
    assert_frame_equal(spikes_df, pd.read_csv(f'{cache}/spikes_df.csv', index_col=0, parse_dates=[0]))
    assert flags.empty

def test_weekend_corr():
    cache = "./cache/test_data_transforms"
    spikes_df = pd.read_csv(f'{cache}/spikes_df.csv', index_col=0, parse_dates=[0])
    wkc = weekend_corr(spikes_df, spikes_df.drop(columns=['end', 'day']).columns)
    assert_frame_equal(wkc, pd.read_csv(f'{cache}/weekend_df.csv', parse_dates=[0]))

def test_ar_method_known():
    cache = "./cache/test_data_transforms"
    weekend_df =  pd.read_csv(f'{cache}/weekend_df.csv', parse_dates=[0])
    states = weekend_df.drop(columns=['date', 'end', 'day', 'weeknum']).columns
    params = Weekday.get_params(weekend_df.copy(), None, states, 'date',
                                [1, 1e5, 1e10, 1e15], None, 10)
    weekday_corr = Weekday.calc_adjustment(params
                                           , weekend_df.copy(),
                                           states, 'date').fillna(0).round(4)
    assert_frame_equal(weekday_corr, pd.read_csv(f'{cache}/weekday_corr.csv', parse_dates=[0]).round(4))

    resid, flags2 = ar_method(weekday_corr, list(states), 2, 4, 2, 1, pd.DataFrame(columns=['date', 'state', 'lags', 'key']))
    assert_frame_equal(resid, pd.read_csv(f'./cache/small_resid.csv', index_col=0, parse_dates=[1], dtype={'date':str, 'state':object}))
    assert_frame_equal(flags2, pd.read_csv(f'./cache/small_flags_ar.csv', index_col=0, parse_dates=[1]))

    rd = pd.read_csv(f'{cache}/resid_4_2.csv')
    resid, flags2 = ar_method(weekday_corr, list(states), 2, 4, 2, 1, rd)
    assert resid.empty
    assert flags2.empty

