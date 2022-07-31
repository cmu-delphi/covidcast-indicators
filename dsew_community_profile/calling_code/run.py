from delphi_utils.flagging import (
    flagger_io, df_start_date)
from delphi_utils import get_structured_logger
import pandas as pd


def dsew_make_df(ref_files):
    df_list = []
    for _, rw in ref_files.iterrows():
        new_df = pd.read_csv(rw.fname, usecols=[0, 1])
        new_df.columns = ['state', rw.name]
        new_df = new_df.set_index(['state'])
        df_list.append(new_df)
    return pd.concat(df_list, axis=1)


def df_func(rel_files, lags):
    df_list = []
    for lag in lags:
        base_df = dsew_make_df(rel_files)
        base_df['lag'] = lag
        df_list.append(base_df)
    return pd.concat(df_list , axis=1)


def run_module(params):
    logger = get_structured_logger(
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))
    start_date = params['flagging']['df_start_date']
    end_date = params['flagging']['df_end_date']
    output_dir = params['flagging']['output_dir']
    input_dir = params['flagging']['input_dir']
    sig = params['flagging']['sig_str']
    raw_df = params['flagging']['raw_df']
    rel_files = rel_files_table(input_dir, start_date, end_date, sig)

    if params['flagging']['flagger_type'] == 'flagger_io':
        flagger_io(params)
    else:
        df_func(rel_files, [0]).to_csv(f'{raw_df}')