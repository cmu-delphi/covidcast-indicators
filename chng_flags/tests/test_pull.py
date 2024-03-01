"""Tests for pulling files."""
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import os
from delphi_chng_flags.pull import (
    pull_lags_data,
    pull_csvs)
import pytest
import shutil



def test_exist_csvs():
    """See if the method returns the proper csv files given certain params"""
    cache = 'cache/test_cache_with_file'
    train_n = 4
    lags = 2
    df_resid, flags_1_df, flags_2_df = pull_csvs(cache,lags,train_n)
    assert_frame_equal(df_resid.reset_index(), pd.read_csv(cache + f'/resid_{train_n}_{lags}.csv', header=0, parse_dates=['date']).drop_duplicates(), check_dtype=False)
    assert_frame_equal(flags_1_df.reset_index(), pd.read_csv(cache + f'/flag_spike_{train_n}_{lags}.csv', header=0, parse_dates=['date']).drop_duplicates(), check_dtype=False)
    assert_frame_equal(flags_2_df.reset_index(), pd.read_csv(cache + f'/flag_ar_{train_n}_{lags}.csv', header=0, parse_dates=['date']).drop_duplicates(), check_dtype=False)

def test_missing_csvs():
    """See if the method returns the proper csv files when missing params"""
    df_resid, flags_1_df, flags_2_df = pull_csvs('cache/test_cache_with_file', 6, 50)
    ref = pd.DataFrame(columns=['date', 'state', 'lags', 'key']).set_index(['lags', 'key', 'date', 'state'])
    assert df_resid.empty is True
    assert flags_1_df.empty is True
    assert flags_2_df.empty is True
    assert df_resid.index.equals(ref.index)
    assert flags_1_df.index.equals(ref.index)
    assert flags_2_df.index.equals(ref.index)



def test_lags_file():
    """Create a new Covid and Denom file from SFTP files using a set start and end date"""
    cache_dir ="./cache/test_cache_create_files"
    lags = [60]
    start_date = "07/01/2021"
    end_date = "07/02/2021"
    df_num, df_den = pull_lags_data(cache_dir, lags, pd.to_datetime(start_date), pd.to_datetime(end_date), reset=True)
    df_num = df_num.reset_index()
    df_den = df_den.reset_index()
    comp_num = pd.read_csv(cache_dir + '/Covid_Intermediate.csv', header=0).reset_index(drop=True)
    comp_den = pd.read_csv(cache_dir + '/Denom_Intermediate.csv', header=0).reset_index(drop=True)
    comp_num.columns = list(comp_num.columns[:2]) + list([pd.to_datetime(x) for x in comp_num.columns[2:]])
    comp_den.columns = list(comp_den.columns[:2]) + list([pd.to_datetime(x) for x in comp_den.columns[2:]])
    assert_frame_equal(comp_num, df_num, check_dtype=False)
    assert_frame_equal(comp_den, df_den, check_dtype=False)
    # remove files
    os.remove(cache_dir + '/Covid.csv')
    os.remove(cache_dir + '/Denom.csv')


def test_add_lags_dates_missing():
    """Create a new Covid and Denom file from SFTP files where there are missing raw files. Also tests adding new lags and days to a dataframe."""
    cache_dir = "./cache/test_cache_create_files"
    shutil.copy(cache_dir + '/Covid_Intermediate.csv', cache_dir + '/Covid.csv')
    shutil.copy(cache_dir + '/Denom_Intermediate.csv', cache_dir + '/Denom.csv')
    start_date = "07/01/2021"
    end_date = "07/05/2021"
    lags = [60, 1]
    df_num, df_den = pull_lags_data(cache_dir, lags, pd.to_datetime(start_date), pd.to_datetime(end_date))
    df_num = df_num.reset_index()
    df_den = df_den.reset_index()
    comp_num = pd.read_csv(cache_dir + '/test_Covid.csv', header=0).reset_index(drop=True).fillna(0).astype(int)
    comp_den = pd.read_csv(cache_dir + '/test_Denom.csv', header=0).reset_index(drop=True).fillna(0).astype(int)
    comp_num.columns = list(comp_num.columns[:2]) + list([pd.to_datetime(x) for x in comp_num.columns[2:]])
    comp_den.columns = list(comp_den.columns[:2]) + list([pd.to_datetime(x) for x in comp_den.columns[2:]])
    assert_frame_equal(comp_num, df_num, check_dtype=False)
    assert_frame_equal(comp_den, df_den, check_dtype=False)
    # remove files
    os.remove(cache_dir + '/Covid.csv')
    os.remove(cache_dir + '/Denom.csv')

def test_start_end_date():
    """same start and end date"""
    cache_dir = "./cache/test_cache_create_files"
    shutil.copy(cache_dir + '/Covid_Intermediate.csv', cache_dir + '/Covid.csv')
    shutil.copy(cache_dir + '/Denom_Intermediate.csv', cache_dir + '/Denom.csv')
    start_date = "07/01/2021"
    end_date = start_date
    lags = [60] #can't use lag 1 bv of missing data issue
    df_num, df_den = pull_lags_data(cache_dir, lags, pd.to_datetime(start_date), pd.to_datetime(end_date))
    df_num = df_num.reset_index()
    df_den = df_den.reset_index()
    comp_num = pd.read_csv(cache_dir + '/test_Covid.csv', header=0).reset_index(drop=True).fillna(0).astype(int)
    comp_den = pd.read_csv(cache_dir + '/test_Denom.csv', header=0).reset_index(drop=True).fillna(0).astype(int)
    comp_num.columns = list(comp_num.columns[:2]) + list([pd.to_datetime(x) for x in comp_num.columns[2:]])
    comp_den.columns = list(comp_den.columns[:2]) + list([pd.to_datetime(x) for x in comp_den.columns[2:]])
    assert_frame_equal(comp_num.query('lags==60').iloc[:, :3], df_num, check_dtype=False)
    assert_frame_equal(comp_den.query('lags==60').iloc[:, :3], df_den, check_dtype=False)
    # remove files
    os.remove(cache_dir + '/Covid.csv')
    os.remove(cache_dir + '/Denom.csv')


def assert_fails():
    """Start date exceeds end date"""
    end_date = "07/20/2021"
    start_date = "08/02/2022"
    with pytest.raises(AssertionError):
        pull_lags_data(cache_dir, lags, pd.to_datetime(start_date), pd.to_datetime(end_date))

    """Start date too early """
    start_date = "07/20/2019"
    end_date = "08/02/2022"
    with pytest.raises(AssertionError):
        df_num, df_den = pull_lags_data(cache_dir, lags, pd.to_datetime(start_date), pd.to_datetime(end_date))

    """End date too late """
    start_date = "07/20/2020"
    end_date = "08/02/9999"
    with pytest.raises(AssertionError):
         pull_lags_data(cache_dir, lags, pd.to_datetime(start_date), pd.to_datetime(end_date))


