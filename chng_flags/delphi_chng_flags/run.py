# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m MODULE_NAME`.
"""
import time
import pandas as pd
from delphi_utils import (
    get_structured_logger,
    Weekday
)
from .data_transform import (
    identify_correct_spikes,
    weekend_corr,
    ar_method)
from .pull import (pull_csvs,
                   pull_lags_data)


def run_module(params):
    """Run Change Flag test module.

    The `params` argument is expected to have the following structure:
    - "common":
        - "export_dir": str, directory to write output
        - "log_exceptions" (optional): bool, whether to log exceptions to file
    - indicator":
        - "input_cache_dir": str, directory in which to cache input data
        - "num_lags": int, the number of lags for the AR model
        - "n_train": int, the number of datapoints to train the AR model on
        - "n_test": int, the number of days to create the residual distribution
        - "n_valid": int, the number of days to rank
        - "lags": list of ints, the windows (lags) to run the flagging program on.
                    A lag of 1 means the data that was received 1 day after the service.
    """
    start_time = time.time()
    logger = get_structured_logger(
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))
    cache_dir = params["indicator"]["input_cache_dir"]
    export_dir = params["common"]["export_dir"]
    lags = params["indicator"]["lags"]
    num_lags = params["indicator"]["num_lags"]
    n_train = params["indicator"]["n_train"]
    n_test = params["indicator"]["n_test"]
    n_valid = params["indicator"]["n_valid"]
    assert num_lags <  n_train
    df_num, df_den = pull_lags_data(cache_dir, lags)
    df_num.reset_index(inplace=True)
    df_den.reset_index(inplace=True)
    resid_df, flags_1_df, flags_2_df = pull_csvs(cache_dir, num_lags, n_train)

    export_files = {'resid_list': resid_df,
                    'flags1_list': flags_1_df,
                    'flags2_list': flags_2_df}
    for lags in lags:
        df_dict = {}
        wname = f"{lags} days"
        df_dict['w_num'] = df_num[df_num.lags == wname].\
            drop(columns=['lags']).set_index('state').T.fillna(0)
        df_dict['w_den'] = df_den[df_den.lags == wname].\
            drop(columns=['lags']).set_index('state').T.fillna(0)
        df_dict['ratio'] = df_dict['w_num'] / df_dict['w_den']
        for key, df in df_dict.items():
            df.columns = df.columns.astype(str)
            df.index = pd.to_datetime(df.index)
            df = df.fillna(0)
            df['day'] = [x.weekday() for x in list(df.index)]
            df['end'] = [x.weekday() in [5, 6] for x in list(df.index)]
            states = df.drop(columns=['end', 'day']).columns
            spikes_df, flags = identify_correct_spikes(df.copy())
            flags = flags.reset_index()
            flags.columns = ['state', 'date', 'val']
            weekend_df = weekend_corr(spikes_df.copy(), states)
            params = Weekday.get_params(weekend_df.copy(), None, states, 'date',
                                        [1, 1e5, 1e10, 1e15], logger, 10)
            weekday_corr = Weekday.calc_adjustment(params
                                                   , weekend_df.copy(),
                                                   states, 'date').fillna(0)
            resid, flags2 = ar_method(weekday_corr.copy(), list(states), num_lags, n_train,
                                      n_test, n_valid, resid_df.query('lags==@lags and key==@key'))
            for tmp_df, ct in zip([resid, flags, flags2],
                                  ['resid_list', 'flags1_list', 'flags2_list']):
                tmp_df['lags'] = lags
                tmp_df['key'] = key
                tmp_df = tmp_df.set_index(['lags', 'key', 'date', 'state'])
                export_files[ct] = pd.concat([export_files[ct], tmp_df])

    export_files['flags2_list'] = export_files['flags2_list'].sort_values(by=['sort_prio'])
    export_files['resid_list'].to_csv(f'{cache_dir}/resid_{n_train}_{num_lags}.csv')
    export_files['flags1_list'].to_csv(f'{cache_dir}/flag_spike_{n_train}_{num_lags}.csv')
    export_files['flags2_list'].to_csv(f'{cache_dir}/flag_ar_{n_train}_{num_lags}.csv')

    export_files['flags1_list'].to_csv(f'{export_dir}/flag_spike_{n_train}_{num_lags}.csv')
    export_files['flags2_list'].to_csv(f'{export_dir}/flag_ar_{n_train}_{num_lags}.csv')

    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    logger.info("Completed indicator run",
                elapsed_time_in_seconds=elapsed_time_in_seconds)
