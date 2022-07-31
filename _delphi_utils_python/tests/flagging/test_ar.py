"""Basic Tests for the AR Files"""
import pandas as pd
from pandas.testing import assert_frame_equal
from delphi_utils.flagging.generate_ar import gen_ar_files
import os
import pytest

start_file = "./flagging"
def out_files_check(ret_files, output_folder):
    """Method to save files. """
    for loc, file in ret_files.items():
        if not file.empty:
            if 'flag' not in loc:
                assert_frame_equal(file, pd.read_csv(f'{start_file}/{output_folder}/{loc}', index_col=0, parse_dates=[0]))
            else:
                assert_frame_equal(file, pd.read_csv(f'{start_file}/{output_folder}/{loc}', index_col=0, parse_dates=[1]))
        else:
            assert_frame_equal(file, pd.read_csv(f'{start_file}/{output_folder}/{loc}', index_col=0))

def test_ar_files():
    """Test for basic AR files. """
    ref_csv = pd.read_csv(f'{start_file}/testbasic/ref_dfs/wkdy_corr.csv', index_col=0, parse_dates=[0]).astype(float)
    out_files = gen_ar_files("", ref_csv, 3, 4,
                 pd.date_range(pd.to_datetime("5/09/2022"), pd.to_datetime("5/10/2022")),
                 pd.date_range(pd.to_datetime("5/10/2022"), pd.to_datetime("5/12/2022")))
    out_files_check(out_files, 'testbasic')

def test_small_n_ar():
    """ Check small values or n_train and ar_lag values. """
    ref_csv = pd.read_csv(f'{start_file}/testbasic/ref_dfs/wkdy_corr.csv', index_col=0, parse_dates=[0]).astype(float)
    out_files = gen_ar_files("", ref_csv, 1, 2,
                     pd.date_range(pd.to_datetime("5/09/2022"), pd.to_datetime("5/10/2022")),
                     pd.date_range(pd.to_datetime("5/10/2022"), pd.to_datetime("5/12/2022")))
    out_files_check(out_files, 'testbasic1')


def test_small_n_ar2():
    """ Make sure that AR_lags is > 0"""
    ref_csv = pd.read_csv(f'{start_file}/testbasic/ref_dfs/wkdy_corr.csv', index_col=0, parse_dates=[0]).astype(float)
    with pytest.raises(AssertionError):
        gen_ar_files("", ref_csv, 0, 2,
                     pd.date_range(pd.to_datetime("5/09/2022"), pd.to_datetime("5/10/2022")),
                     pd.date_range(pd.to_datetime("5/10/2022"), pd.to_datetime("5/12/2022")))


def test_small_n_ar2():
    """ Make sure that n_train is > 2"""
    ref_csv = pd.read_csv(f'{start_file}/testbasic/ref_dfs/wkdy_corr.csv', index_col=0, parse_dates=[0]).astype(float)
    with pytest.raises(AssertionError):
        gen_ar_files("", ref_csv, 1, 0,
                     pd.date_range(pd.to_datetime("5/09/2022"), pd.to_datetime("5/10/2022")),
                     pd.date_range(pd.to_datetime("5/10/2022"), pd.to_datetime("5/12/2022")))

def test_ar_greater_train():
    """ Test functionality when ar lags is greater than n_train."""
    ref_csv = pd.read_csv(f'{start_file}/testbasic/ref_dfs/wkdy_corr.csv', index_col=0, parse_dates=[0]).astype(float)
    out_files = gen_ar_files("", ref_csv, 4, 2,
                             pd.date_range(pd.to_datetime("5/09/2022"), pd.to_datetime("5/10/2022")),
                             pd.date_range(pd.to_datetime("5/10/2022"), pd.to_datetime("5/12/2022")))
    out_files_check(out_files, 'testbasic2')

def test_input_dates_swap():
    """ Test when resid and eval dateranges are swapped. """
    ref_csv = pd.read_csv(f'{start_file}/testspikes/ref_dfs/wkdy_corr.csv', index_col=0, parse_dates=[0]).astype(float)
    out_files = gen_ar_files("", ref_csv, 4, 2,
                             pd.date_range(pd.to_datetime("5/10/2022"), pd.to_datetime("5/12/2022")),
                             pd.date_range(pd.to_datetime("5/09/2022"), pd.to_datetime("5/10/2022")))
    out_files_check(out_files, 'testspikes')


def test_input_dates_before():
    """ Test if ar_lags + n_train + resid_start > the start date of the dataframe."""
    ref_csv = pd.read_csv(f'{start_file}/testbasic/ref_dfs/wkdy_corr.csv', index_col=0, parse_dates=[0]).astype(float)
    with pytest.raises(AssertionError):
        _ = gen_ar_files("", ref_csv, 4, 2,
                                 pd.date_range(pd.to_datetime("5/05/2022"), pd.to_datetime("5/10/2022")),
                                pd.date_range(pd.to_datetime("5/10/2022"), pd.to_datetime("5/12/2022")))

def test_input_dates_miss():
    """ Test if resid input dates are before the dataframe start. """
    ref_csv = pd.read_csv(f'{start_file}/testbasic/ref_dfs/wkdy_corr.csv', index_col=0, parse_dates=[0]).astype(float)
    with pytest.raises(AssertionError):
        _ = gen_ar_files("", ref_csv, 4, 2,
                                 pd.date_range(pd.to_datetime("4/05/2022"), pd.to_datetime("4/10/2022")),
                                pd.date_range(pd.to_datetime("5/10/2022"), pd.to_datetime("5/12/2022")))


def test_na_input():
    ref_csv = pd.read_csv(f'{start_file}/testna/ref_dfs/wkdy_corr.csv', index_col=0, parse_dates=[0]).astype(float)
    out_files = gen_ar_files("", ref_csv, 3, 4,
                             pd.date_range(pd.to_datetime("5/09/2022"), pd.to_datetime("5/10/2022")),
                             pd.date_range(pd.to_datetime("5/10/2022"), pd.to_datetime("5/12/2022")))
    out_files_check(out_files, 'testna')









