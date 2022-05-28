# -*- coding: utf-8 -*-
import glob
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from delphi_changehc.load_data import load_chng_data

#temporary loccation is in cachedir for these 4 files
# we want to
#TODO: pull numerator and denom file from s3, fix, and re-upload
# TODO: check files and windows that are missing and then append them
# TODO: optimze later
def pull_lags_data(cache_dir, windows):
    """Import the numerator and the denomenator counts files and update them
      as needed using data from the SFTP. """
    def make_df(file_df, windows):
        df_list = []
        for i, df in file_df.iterrows():
            new_df = pd.read_csv(df.values[0], header=None)
            new_df.columns = ['date', 'state', 'counts']
            file_date = df.name
            new_df['date'] = pd.to_datetime(new_df['date'], format='%Y%m%d', errors='coerce')
            dates_list = [file_date - pd.Timedelta(days=x) for x in windows]
            new_df = new_df.query('date.isin(@dates_list)')
            new_df['date'] = (file_date - new_df['date'])
            new_df = new_df[pd.to_numeric(new_df['state'], errors='coerce').notnull()]

            new_df['counts'] = new_df['counts'].replace("3 or less", 2).astype(int)
            new_df['state'] = new_df.state.astype(int).astype(str).str.zfill(5).str[:2].astype(int)
            new_df = new_df.query('state<57')

            new_df = new_df.groupby(['date', 'state']).sum().sort_values(by=['date', 'state'])
            new_df.columns = [file_date]
            df_list.append(new_df)
        tmp = pd.concat(df_list, axis=1)
        tmp.to_csv(file_str + "_" + name)
        return tmp
    def file_update(cache_dir, str_file, windows):
        dates_range = pd.date_range(pd.to_datetime('20210301', format="%Y%m%d"), datetime.date.today())
        df = pd.DataFrame(columns=['date', 'state'])
        list_fname = glob.glob(f'{cache_dir}/{str_file}.csv')
        if list_fname == 1:
            num_fname = list_fname[0]
            df = pd.read_csv(num_fname).fillna(0)
            df = df.set_index(['lags', 'state'])
        existing_windows = np.unique(df['date'])
        existing_dates = df.columns[2:]
        missing_windows = [x not in existing_windows for x in windows]
        missing_dates = [pd.to_datetime(x, format="%Y%m%d") not in existing_dates for x in dates_range]

        rel_files = pd.DataFrame()
        rel_files['fname'] = glob.glob(f'{cache_dir}/*{str_file}.dat.gz')
        rel_files['fdate'] = pd.to_datetime(
            rel_files['fname'].str.rsplit('/', n=1, expand=True)[1].str.split('_', n=1, expand=True)[0],
            format='%Y%m%d', errors='coerce')
        rel_files = rel_files.set_index('fdate')
        merge_files = pd.DataFrame(index=dates_range)
        rel_files = merge_files.merge(rel_files, how='outer', left_index=True, right_index=True).fillna(method='ffill')

        if len(missing_windows) > 0:
            sel_rel_files = rel_files.query('index in @existing_dates').fname.sort_index()
            df = pd.concat([df, make_df(sel_rel_files, missing_windows)], axis=0)
        sel_rel_files = rel_files.query('index not in @existing_dates').fname.sort_index()
        df = pd.concat([df, make_df(sel_rel_files, windows)], axis=1)
        df.to_csv(f'{cache_dir}/{str_file}.csv')
        return df
    df_num = file_update('Covid')
    df_den = file_update('Denom')
    return df_num, df_den


def pull_csvs(cache_dir, num_lags, num_train):
    resid_df = pd.DataFrame()
    flags_1_df = pd.DataFrame()
    flags_2_df = pd.DataFrame()
    files_list = glob.glob(f'{cache_dir}/fresid_{n_train}_{num_lags}.csv')
    if len(files_list) == 1:
        resid_df = pd.read_csv(files_list[0])
    files_list = glob.glob(f'{cache_dir}/flag_spike_{n_train}_{num_lags}.csv')
    if len(files_list) == 1:
        flags_1_df = pd.read_csv(files_list[0])
    files_list = glob.glob(f'{cache_dir}/flag_ar_{n_train}_{num_lags}.csv')
    if len(files_list) == 1:
        flags_2_df = pd.read_csv(files_list[0])
    return resid_df, flags_1_df, flags_2_df
