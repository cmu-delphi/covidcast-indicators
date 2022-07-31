"""Basic Tests for the IO functionality """
import pandas as pd
from pandas.testing import assert_frame_equal
import delphi_utils.flagging.flag_io as fio
from unittest import mock
import pytest
import filecmp
import tempfile
import shutil

tempdir = "./flagging/test_tmp"
params = {
  "common": {
    "log_filename": "dfs.log"
  },
  "flagging": {
    "n_train": 2,
    "ar_lags": 1,
    "df_start_date":"05/01/2022",
    "df_end_date":"05/30/2022",

    "resid_start_date":"05/10/2022",
    "resid_end_date":"05/12/2022",
    "eval_start_date":"05/12/2022",
    "eval_end_date":"05/14/2022",

    "input_dir": "./receiving",

    "output_dir": tempdir,
    "lags": ['0', '1','var'],

    "raw_df": "./flagging/ref_files/basic_lags.csv",
    "sig_fold": "test",
    "sig_str": "basic_sig",

    "flagger_type": "",
    "sig_type": "local",
    "remote": False,

  }
}

def test_local_regen():
  """ Test flagger_df method with clean folder"""
  global params
  fio.flagging(params)
  filecmp.cmp(f'{tempdir}/test', './flagging/test_io_output/ref', shallow=True)
  shutil.rmtree(f'{tempdir}/test')

def test_local_regen_already_exists():
  """ Test flagger_df method with files that already exist"""
  global params
  params['sig_str'] = 'flagger_df'
  fio.flagging(params)
  fio.flagging(params)
  filecmp.cmp(f'{tempdir}/test', './flagging/test_io_output/ref', shallow=True)
  shutil.rmtree(f'{tempdir}/test')

def test_local_reuse_clean():
  """ Test flagger_io method with clean folder."""
  global params
  params['flagging']["flagger_type"] = "flagger_io"
  fio.flagging(params)
  filecmp.cmp(f'{tempdir}/test', './flagging/test_io_output/ref', shallow=True)
  shutil.rmtree(f'{tempdir}/test')

def test_local_reuse_already_exists():
  """ Test flagger_io method with existing folders."""
  global params
  fio.flagging(params)
  params['flagging']["flagger_type"] = "flagger_io"
  fio.flagging(params)
  filecmp.cmp(f'{tempdir}/test', './flagging/test_io_output/ref', shallow=True)
  shutil.rmtree(f'{tempdir}/test')

def test_local_reuse_already_exists2():
  """ Test flagger_io method with existing folders one folder deleted."""
  global params
  fio.flagging(params)
  params['flagging']["flagger_type"] = "flagger_io"
  shutil.rmtree(f'{tempdir}/test/basic_sig/local/window_var/train_2_lags_1')
  fio.flagging(params)
  filecmp.cmp(f'{tempdir}/test', './flagging/test_io_output/ref', shallow=True)
  shutil.rmtree(f'{tempdir}/test')

#Right now, you need to manually verify remote tests
# def test_local_regen():
#   """ Test flagger_df method with clean folder, you need to check these individually """
#   global params
#   params['flagging']["remote"] = True
#   fio.flagging(params)
#
# def test_local_reuse():
#   global params
#   params['flagging']["remote"] = True
#   params['flagging']["flagger_type"] = "flagger_io"
#   fio.flagging(params)


def test_rel_files_table():
  global params
  start_date = '05/01/2022'
  end_date = '05/04/2022'
  input_dir = './flagging/receiving2'
  sig = "state_doses_admin_7dav"
  rel_files = fio.rel_files_table(input_dir, start_date, end_date, sig)
  assert len(rel_files)==4
