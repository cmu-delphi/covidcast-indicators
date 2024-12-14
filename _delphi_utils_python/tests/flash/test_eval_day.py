"""Tests for eval_day.py"""
import mock
import pandas as pd
from delphi_utils.flash_eval.eval_day import (flash_eval)


def test_flash_input():
    "Simple test for main flash-eval method with known output."
    mock_logger = mock.Mock()
    input_df = pd.read_csv('flash_ref/test_flash.csv',
                                        index_col=0, parse_dates=[0], header=0)

    lag = 1
    day = pd.to_datetime("1/1/2023", format="%m/%d/%Y")
    signal = './confirmed_incidence_num'
    params = {'flash': {'support':[0,1]}}
    initial_7_day_file = pd.read_csv(f'flash_ref/{signal}/last_7_1.csv',
                                                  index_col=0, parse_dates=[0], header=0)
    last_7, type_of_outlier = flash_eval(lag, day, input_df, signal, params, logger=mock_logger, local=True)
    initial_7_day_file.to_csv(f'flash_ref/{signal}/last_7_1.csv')
