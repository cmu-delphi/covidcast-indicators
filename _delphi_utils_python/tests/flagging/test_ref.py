"""Basic Tests for the Reference Files"""
import pandas as pd
from pandas.testing import assert_frame_equal
from delphi_utils.flagging.generate_reference import gen_ref_dfs
from unittest import mock
import pytest

start_file = "./flagging"
@mock.patch("delphi_utils.logger")
def basic_struct(ref_csv, output_folder, mock_logger):
    ret_files = gen_ref_dfs(ref_csv, mock_logger)
    for loc, file in ret_files.items():
        if not file.empty:
            if 'flag' not in loc:
                assert_frame_equal(file, pd.read_csv(f'{start_file}/{output_folder}/{loc}', index_col=0, parse_dates=[0]))
            else:
                assert_frame_equal(file, pd.read_csv(f'{start_file}/{output_folder}/{loc}', index_col=0, parse_dates=[2]))
        else:
            assert_frame_equal(file, pd.read_csv(f'{start_file}/{output_folder}/{loc}', index_col=0))

def test_basic_test():
    """See if the method returns the proper csv files given basic df case"""
    fname ='basic.csv'
    ref_csv = pd.read_csv(f'{start_file}/ref_files/{fname}', index_col=0, parse_dates=[0])
    basic_struct(ref_csv, 'testbasic')

def test_empty():
    with pytest.raises(AssertionError):
        ref_csv = pd.DataFrame()
        basic_struct(ref_csv, "")

def test_small_df_test():
    """See if the method returns the proper csv files given small df (no weekend corr)"""
    fname = 'basic.csv'
    with pytest.raises(AssertionError):
        ref_csv = pd.read_csv(f'{start_file}/ref_files/{fname}', index_col=0, parse_dates=[0]).iloc[:2, :]
        basic_struct(ref_csv, "")

def test_small_df_test2():
    """See if the method returns the proper csv files given small df (no weekend corr)"""
    fname = 'basic.csv'
    ref_csv = pd.read_csv(f'{start_file}/ref_files/{fname}', index_col=0, parse_dates=[0]).iloc[:7, :]
    basic_struct(ref_csv, 'testsmall')

def test_simultaneous_large_spikes():
    """See if the method returns the proper results if there are simultaneous spikes in a row"""
    fname = 'spikes.csv'
    ref_csv = pd.read_csv(f'{start_file}/ref_files/{fname}', index_col=0, parse_dates=[0], header=0)
    basic_struct(ref_csv, 'testspikes')


def test_cvxpy_fail():
    """See if the method returns expected results when cvxpy fails"""
    fname = 'cvxpy_fail.csv'
    ref_csv = pd.read_csv(f'{start_file}/ref_files/{fname}', index_col=0, parse_dates=[0]).dropna()
    basic_struct(ref_csv, 'cvxpyfail')

def test_missing_dates():
    "See if fill is correct if df is missing dates"
    fname = 'basic.csv'
    ref_csv = pd.read_csv(f'{start_file}/ref_files/{fname}', index_col=0, parse_dates=[0], header=0).iloc[[1,3, 20], :]
    basic_struct(ref_csv, 'testmissing')

def test_na_behavior():
    "See if na behavior is expected"
    fname = 'na_files.csv'
    ref_csv = pd.read_csv(f'{start_file}/ref_files/{fname}', index_col=0, parse_dates=[0]).dropna()
    basic_struct(ref_csv, 'testna')

def test_multiple_dates():
    "See if removing duplicates is possible."
    fname = 'basic.csv'
    a = pd.read_csv(f'{start_file}/ref_files/{fname}', index_col=0, parse_dates=[0], header=0)
    ref_csv = pd.concat([a, a], axis=0)
    basic_struct(ref_csv, 'testmul')


def test_multiple_dates2():
    "Throw error if there are conflicting values for the same date."
    fname = 'basic.csv'
    a = pd.read_csv(f'{start_file}/ref_files/{fname}', index_col=0, parse_dates=[0], header=0)
    ref_csv = pd.concat([a, a*7], axis=0)
    with pytest.raises(AssertionError):
        basic_struct(ref_csv, '')


