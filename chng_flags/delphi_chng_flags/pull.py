# -*- coding: utf-8 -*-
import glob
from datetime import datetime, timedelta
import pandas as pd
import numpy as np


def pull_lags_data(cache_dir, lags):
    """Import the numerator and the denomenator counts files and update them
      as needed using data from the SFTP. """

    def make_df(file_df, lags):
        df_list = []
        for file_date, df in zip(file_df.index, file_df):
            print(file_date)
            new_df = pd.read_csv(df, header=None)
            new_df.columns = ['date', 'state', 'counts']
            new_df['date'] = pd.to_datetime(new_df['date'], format='%Y%m%d', errors='coerce')
            dates_list = [file_date - pd.Timedelta(days=x) for x in lags]
            new_df = new_df.query('date.isin(@dates_list)')
            new_df['lags'] = (file_date - new_df['date'])
            new_df = new_df[pd.to_numeric(new_df['state'], errors='coerce').notnull()]

            new_df['counts'] = new_df['counts'].replace("3 or less", 2).astype(int)
            new_df['state'] = new_df.state.astype(int).astype(str).str.zfill(5).str[:2].astype(int)
            new_df = new_df.query('state<57')

            new_df = new_df.drop(columns=['date']).groupby(['lags', 'state']).sum().sort_values(by=['lags', 'state'])
            new_df.columns = [file_date]
            df_list.append(new_df)
        tmp = pd.concat(df_list, axis=1)
        return tmp

    def file_update(cache_dir, str_file, lags):
        # TODO Fix date below
        dates_range = pd.date_range(pd.to_datetime('20210301', format="%Y%m%d"), datetime.date.today())
        df = pd.DataFrame(columns=['lags', 'state'])
        list_fname = glob.glob(f'{cache_dir}/{str_file}.csv')
        if len(list_fname) == 1:
            num_fname = list_fname[0]
            df = pd.read_csv(num_fname, header=0).fillna(0)
        existing_lags = [int(x.split()[0]) for x in np.unique(df['lags'])]
        df.lags = pd.to_timedelta(df.lags)
        df = df.set_index(['lags', 'state'])
        df.columns = pd.to_datetime(df.columns)
        existing_dates = df.columns
        missing_lags = list(filter(lambda x: x not in existing_lags, lags))
        missing_dates = list(filter(lambda x: pd.to_datetime(x, format="%Y%m%d") not in existing_dates, dates_range))

        rel_files = pd.DataFrame()
        rel_files['fname'] = glob.glob(f'{cache_dir}/*{str_file}.dat.gz')
        rel_files['fdate'] = pd.to_datetime(
            rel_files['fname'].str.rsplit('/', n=1, expand=True)[1].str.split('_', n=1, expand=True)[0],
            format='%Y%m%d', errors='coerce')
        rel_files = rel_files.set_index('fdate')
        merge_files = pd.DataFrame(index=dates_range)
        rel_files = merge_files.merge(rel_files, how='outer', left_index=True, right_index=True).fillna(method='ffill')
        if (len(missing_lags) > 0) and len(existing_dates) > 0:
            sel_rel_files = rel_files.query('index in @existing_dates').fname.sort_index()
            df = pd.concat([df, make_df(sel_rel_files, missing_lags)])

        sel_rel_files = rel_files.query('index in @missing_dates').fname.sort_index()
        if sel_rel_files.shape[0] > 0:
            df = pd.concat([df, make_df(sel_rel_files, lags)], axis=1)
        df.to_csv(f'{cache_dir}/{str_file}.csv')
        return df

    df_num = file_update(cache_dir, 'Covid', lags)
    df_den = file_update(cache_dir, 'Denom', lags)
    return df_num, df_den


def pull_csvs(cache_dir, num_lags, num_train):
    df_resid = pd.DataFrame(columns=['date', 'state', 'lags', 'key'])
    flags_1_df = pd.DataFrame(columns=['date', 'state', 'lags', 'key'])
    flags_2_df = pd.DataFrame(columns=['date', 'state', 'lags', 'key'])
    files_list = glob.glob(f'{cache_dir}/resid_{n_train}_{num_lags}.csv')
    if len(files_list) == 1:
        resid_df = pd.read_csv(files_list[0])
        resid_df['date'] = pd.to_datetime(resid_df['date'])
    files_list = glob.glob(f'{cache_dir}/flag_spike_{n_train}_{num_lags}.csv')
    if len(files_list) == 1:
        flags_1_df = pd.read_csv(files_list[0])
        flags_1_df['date'] = pd.to_datetime(flags_1_df['date'])
    files_list = glob.glob(f'{cache_dir}/flag_ar_{n_train}_{num_lags}.csv')
    if len(files_list) == 1:
        flags_2_df = pd.read_csv(files_list[0])
        flags_2_df['date'] = pd.to_datetime(flags_2_df['date'])
    df_resid = df_resid.set_index(['lags', 'key', 'date', 'state'])
    flags_1_df = flags_1_df.set_index(['lags', 'key', 'date', 'state'])
    flags_2_df = flags_2_df.set_index(['lags', 'key', 'date', 'state'])
    return df_resid, flags_1_df, flags_2_df