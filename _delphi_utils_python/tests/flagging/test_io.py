"""Basic Tests for the IO functionality """
import delphi_utils.flagging.flag_io as fio
import filecmp
import shutil

tempdir = "./flagging/test_tmp"
params = {
  "common": {
    "log_filename": "dfs.log"
  },
  "flagging": [{
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

  }]
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
  params['flagging'][0]["flagger_type"] = "flagger_io"
  fio.flagging(params)
  filecmp.cmp(f'{tempdir}/test', './flagging/test_io_output/ref', shallow=True)
  shutil.rmtree(f'{tempdir}/test')

def test_local_reuse_already_exists():
  """ Test flagger_io method with existing folders."""
  global params
  fio.flagging(params)
  params['flagging'][0]["flagger_type"] = "flagger_io"
  fio.flagging(params)
  filecmp.cmp(f'{tempdir}/test', './flagging/test_io_output/ref', shallow=True)
  shutil.rmtree(f'{tempdir}/test')

def test_local_reuse_already_exists2():
  """ Test flagger_io method with existing folders one folder deleted."""
  global params
  fio.flagging(params)
  params['flagging'][0]["flagger_type"] = "flagger_io"
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

def test_multi_sig():
    """Test generating params file for multiple signals in one source."""
    source_sig = {
      "google-symptoms": ["s01_raw_search", "s01_smoothed_search"],
      # "chng": ["smoothed_outpatient_cli", "smoothed_adj_outpatient_cli"],
      # "jhu-csse": ["confirmed_incidence_num", "confirmed_incidence_prop",
      #              "confirmed_7dav_incidence_num"],
      # "fb-survey": ["smoothed_wwearing_mask_7d", "smoothed_wcli",
      #               "raw_wcli"],
      # "doctor-visits": ["smoothed_adj_cli"],
      # "quidel": ["covid_ag_smoothed_pct_positive"]
    }
    pjson = {"common": {
      "export_dir": "./receiving",
      "log_filename": "dfs.log"
    }}
    fl_list = []
    for key, value_list in source_sig.items():
      for value in value_list:
        fl_val = {
          "n_train": 2,
          "ar_lags": 2,
          "df_start_date": "05/12/2022",
          "df_end_date": "05/30/2022",

          "resid_start_date": "05/20/2022",
          "resid_end_date": "05/22/2022",
          "eval_start_date": "05/23/2022",
          "eval_end_date": "05/30/2022",

          "input_dir": "./receiving",

          "output_dir": "./flagging/multitest",
          "lags": [30, 60],

          "raw_df": f'./flagging/raw_dfs/all_lags_{key}-{value}.csv',
          "sig_fold": key,
          "sig_str": value,

          "sig_type": "api",
          "remote": False,
          "flagger_type": "flagger_df"
        }
        fl_list.append(fl_val)

    pjson['flagging'] = fl_list
    fio.flagging(pjson)

    fl_value = fl_list[0]
    filecmp.cmp(fl_value['output_dir'], './flagging/test_multioutput', shallow=True)
    shutil.rmtree(fl_value['output_dir'])
