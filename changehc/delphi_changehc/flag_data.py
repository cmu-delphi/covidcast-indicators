"""A file in every indicator pipeline to create the appropriate raw dfs."""
import os.path
from datetime import  timedelta, datetime

import glob
from delphi_utils import get_structured_logger
from delphi_utils.flagging.flag_io import (raw_df_from_api, rel_files_table, params_meta)
import pandas as pd
from .run import retrieve_files


def chc_make_df(ref_files, lags):
    """Create dfs specific to the change raw data."""
    df_list = []
    cols = ['ak', 'al', 'ar', 'az', 'ca', 'co',
            'ct', 'dc', 'de', 'fl', 'ga', 'hi',
            'ia', 'id', 'il', 'in', 'ks', 'ky',
            'la', 'ma', 'md', 'me', 'mi', 'mn',
            'mo', 'ms', 'mt', 'nc', 'nd', 'ne',
            'nh', 'nj', 'nm', 'nv', 'ny', 'oh',
            'ok', 'or', 'pa', 'ri', 'sc', 'sd',
            'tn', 'tx', 'ut', 'va', 'vt', 'wa',
            'wi', 'wv', 'wy']
    # change iterand to over the dates we needs
    for _, rw in ref_files.iterrows():
        new_df = pd.read_csv(rw.fname, header=None, dtype={
            0: 'str',
            1: 'str',
            2: 'str'
        }, parse_dates=[0])
        new_df.columns = ['date', 'state', 'counts']
        new_df['date'] = pd.to_datetime(new_df['date'], format='%Y%m%d', errors='coerce')
        ilist = []
        for x in lags:
            tmp_date = rw.name - pd.Timedelta(days=rw.win_sub + x)
            sel_df = new_df[new_df.date == tmp_date]
            sel_df = sel_df[pd.to_numeric(sel_df['state'], errors='coerce').notnull()]
            sel_df['state'] = sel_df.state.astype(int).astype(str).str.zfill(5).str[:2].astype(int)
            sel_df = sel_df.query('0<state<57')
            sel_df['counts'] = sel_df['counts'].replace("3 or less", 2).astype(int)
            sel_df = sel_df.drop(columns=['date']).groupby(['state']). \
                sum().sort_values(by=['state']).T
            sel_df.index = [tmp_date]
            sel_df['lag'] = x
            sel_df = sel_df.reset_index()
            ilist.append(sel_df)
        new_df = pd.concat(ilist, axis=0)
        df_list.append(new_df)
    tmp = pd.concat(df_list, axis=0)
    if len(tmp.columns) != 53:
        return ValueError('There are too many or too few columns than predefined.')
    tmp.columns = ['date'] + cols + ['lag']
    return tmp

def create_df(fl_val, rel_str, params):
    """Create dfs CHC specific for raw or ratio."""
    s_date = pd.to_datetime(fl_val['df_start_date'])
    e_date = pd.to_datetime(fl_val['df_end_date'])
    max_lag = max(fl_val['lags'])
    min_lag = min(fl_val['lags'])
    max_date = s_date + timedelta(min_lag)
    min_date = e_date + timedelta(max_lag)
    # Future: Reuse raw.dfs by pulling remote file per lag and return relevant dates + missing dates
    # creates the table
    for date in pd.date_range(max_date, min_date):
        if len(glob.glob(f"./cache/{date.strftime('%Y%m%d')}*{rel_str}*")) == 0:
            # Future: figure this out (logger)
            logger = get_structured_logger(
                __name__, filename=params["common"].get("log_filename"),
                log_exceptions=params["common"].get("log_exceptions", True))
            retrieve_files(params, date.strftime("%Y%m%d"), logger)
    rel_files = rel_files_table("./cache", max_date, min_date, rel_str)
    base_df = chc_make_df(rel_files, fl_val['lags'])
    dr = [x.to_pydatetime() for x in list(pd.date_range(s_date, e_date))]
    base_df = base_df[base_df['date'] in dr]
    base_df = base_df.set_index('date')
    return base_df

def flag_dfs(params):
    """Create the raw dataframes for the flagging module."""
    df_list = []
    for i, fl_val in enumerate(params['flagging']):
        if params['flagging_meta']['generate_dates']:
            fl_val = params_meta(fl_val)
            params['flagging'][i] = fl_val
        if fl_val['sig_type'] == 'raw':
            df_list.append(create_df(fl_val, fl_val['sig_str'], params))
        elif fl_val['sig_type'] == 'ratio':
            num = create_df(fl_val, fl_val['sig_str'], params).fillna(0)
            den = create_df(fl_val, fl_val['sig_den'], params).fillna(0)
            ratio_df = num/den
            ratio_df['lag'] = den['lag']
            df_list.append(ratio_df)
        else:
            df_list.append(raw_df_from_api(fl_val))
    return params, df_list
