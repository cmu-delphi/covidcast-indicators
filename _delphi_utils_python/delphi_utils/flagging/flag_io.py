"""Functions related to determining which files to generate with which parameters."""
import os
import glob
from datetime import timedelta, date
import numpy as np
import pandas as pd
import boto3
import covidcast
from .generate_reference import gen_ref_dfs
from .generate_ar import gen_ar_files
from .. import (
    get_structured_logger,
)


def gen_files(gen_ref_dict:dict, gen_ar_dict:dict, logger) -> dict:
    """
    Generate necessary files.

    This method takes in dictionaries that correspond to the files needed to
    be generated that are either reference files or the autoregression files.

    The output is a complete dictionary of all the file names and their respective dataframes.
    """
    files_dict = {}
    for key, params in gen_ref_dict.items():
        df_dict = gen_ref_dfs(params['raw_df'].query("lag==@params['lag']")
                              .drop(columns=['lag']), logger)
        files_dict.update(df_dict)
        files_dict.update(gen_ar_files(key,
                   df_dict['ref_dfs/wkdy_corr.csv'],
                   params['ar_lag'], params['n_train'],
                   params['resid_dist_dates'], params['eval_dates']))
    for key, params in gen_ar_dict.items():
        ar_dict = gen_ar_files(key, params['input_df'],
                                       params['ar_lag'], params['n_train'],
                                       params['resid_dist_dates'], params['eval_dates'])
        files_dict.update(ar_dict)
    return files_dict


def missing_local_files(df: pd.DataFrame, flag_p: dict, flag_meta: dict) -> (dict, dict):
    """
    Find files that are missing when this method is running on a local filesystem.

    Note that there are different methods for local and remote because the s3 filestructure is flat.
    Output are two dictionaries that correspond to the missing files that are
    either reference files or AR files.
    """
    def files_exist(flag_p:dict, lag:int) -> dict:
        """Create file prefix (to save file in correct location) & find missing ref/ar files."""
        pfold = f"train_{flag_meta['n_train']}_lags_{flag_meta['ar_lags']}"
        dates_dict = {'raw.csv': [],
                      'ar_output.csv': []}
        prefix = f'{flag_meta["output_dir"]}/{flag_p["sig_str"]}/window_{lag}'
        file_list = [f'{prefix}/ref_dfs/raw.csv',
                     f'{prefix}/{pfold}/ar_output.csv']
        _ = [dates_dict.update({x.split('/')[-1]:
                    pd.read_csv(x, index_col=0, parse_dates=[0]).index})
         for x in file_list if os.path.exists(x)]
        return dates_dict, prefix

    gen_dict, gen_ar_dict = {}, {}
    base_dict = {'ar_lag': flag_meta['ar_lags'],
                 'n_train': flag_meta['n_train'],
                 'resid_dist_dates': pd.date_range(pd.to_datetime(flag_p["resid_start_date"]),
                                                   pd.to_datetime(flag_p["resid_end_date"])),
                 'eval_dates': pd.date_range(pd.to_datetime(flag_p["eval_start_date"]),
                                             pd.to_datetime(flag_p["eval_end_date"]))}
    for lag in flag_p['lags']:
        lag_name = f'window_{lag}'
        pfold = f"train_{flag_p['n_train']}_lags_{flag_p['ar_lags']}"
        dates_dict, prefix = files_exist(flag_p, lag)
        if (flag_p["df_start_date"] not in dates_dict['raw.csv']) or\
                (flag_p["df_end_date"] not in dates_dict['raw.csv']):
            base_dict.update({'raw_df': df,
                              'lag': lag,
                              'sig_path': f"{prefix.split('/')},{pfold}"})
            gen_dict[f"{lag_name}/{pfold}"] = base_dict
        else:
            final_df = pd.read_csv(f'{prefix}/{lag_name}/ref_dfs/wkdy_corr.csv',
                                   index_col=0, parse_dates=[0])
            raw_dates = dates_dict['ar_output.csv']
            if not ((len(raw_dates) != 0) and
                     ((flag_p["eval_start_date"] in raw_dates)
                      and (flag_p["eval_end_date"] in raw_dates))):
                base_dict.update({'input_df': final_df})
                gen_ar_dict[f'{lag_name}/{pfold}'] = base_dict
    return gen_dict, gen_ar_dict


def missing_remote_files(df, flag_p, flag_meta):
    """Find missing remote files."""
    conn = boto3.client('s3',
                        aws_access_key_id=flag_meta["aws_access_key_id"],
                        aws_secret_access_key=flag_meta["aws_secret_access_key"])
    remote_files = conn.list_objects(Bucket='delphi-covidcast-public',
                                     Prefix="flags-dev", Delimiter='')['Contents']
    gen_dict, gen_ar_dict = {}, {}
    g = pd.DataFrame([key['Key'].split('/') for key in remote_files]).replace('', np.nan)
    g = g.query("@g[1]==@flag_p['sig_fold'] and  \
                @g[2]==@flag_p['sig_str'] and  \
                @g[3]==@flag_p['sig_type']")
    base_dict = {'ar_lag': flag_meta['ar_lags'],
                 'n_train': flag_meta['n_train'],
                 'resid_dist_dates': pd.date_range(pd.to_datetime(flag_p["resid_start_date"]),
                                                   pd.to_datetime(flag_p["resid_end_date"])),
                 'eval_dates': pd.date_range(pd.to_datetime(flag_p["eval_start_date"]),
                                             pd.to_datetime(flag_p["eval_end_date"]))}
    for lag in flag_p["lags"]:
        proc_df = df.query('lag==@lag')#.drop(columns=['lag'])
        if not proc_df.empty:
            lag_name = f'window_{lag}'
            pfold = f"train_{flag_meta['n_train']}_lags_{flag_meta['ar_lags']}"
            rel_files = g[g[4] == lag_name]
            base_dict.update({'raw_df': proc_df,
                              'lag': lag,
                              'sig_path': f"{flag_p['sig_fold']},{flag_p['sig_str']},"+
                                          f"{flag_p['sig_type']},{lag_name},{pfold}"})
            if rel_files.empty:
                gen_dict[lag_name] = base_dict
            else:
                def raw_dates_from_file(conn, fname, files):
                    key = files[files[6] == fname]
                    data = conn.get_object(Bucket='delphi-covidcast-public',
                                           Key='/'.join(key.values[0]))
                    return pd.read_csv(data['Body'], index_col=0, parse_dates=[0], header=0)
                raw_df = raw_dates_from_file(conn, 'raw.csv', rel_files)
                raw_dates = raw_df.index
                # The raw dataframe was missing dates
                if (flag_p["df_start_date"] not in raw_dates) or (
                        flag_p["df_end_date"] not in raw_dates):
                    gen_dict[lag_name] = base_dict
                else:
                    res_files = g.query("@g[5]==@pfold")
                    final_df = raw_dates_from_file(conn, 'wkdy_corr.csv', rel_files)
                    base_dict.update({'input_df': final_df})
                    if res_files.empty:
                        gen_ar_dict[f'{lag_name}/{pfold}'] = base_dict
                    if not ((res_files.empty) and
                            ((flag_p["eval_start_date"] in raw_dates)
                             and (flag_p["eval_end_date"] in raw_dates))):
                        gen_ar_dict[f'{lag_name}/{pfold}'] = base_dict
    return gen_dict, gen_ar_dict

def log_flag(files, loc_list, logger):
    """Log method important for visualization and Slack integration."""
    html_link = "<https://ananya-joshi-visapp-vis-523f3g.streamlitapp.com/?params="
    for name, flags in files.items():
        if "flagging" in name and not flags.empty:
            event = ""
            p_text = ""
            for grp, df in flags.groupby('date'):
                dt = grp.strftime('%Y/%m/%d')
                p_text += f"*{dt}* \n"
                df = df.sort_values(by=['val', 'state'])
                for i, fl_df in df.reset_index(drop=True).iterrows():
                    if "spike" in name:
                        event = "Large Change Spike Flag"
                        start_link = f"{html_link}{','.join(loc_list[:3])},{dt},{fl_df.state}"
                        p_text += f"\t{start_link}|*{fl_df.state}*>\n"
                    elif "ar" in name:
                        event = "Autoregressive Model Flag"
                        start_link = f"{html_link}{','.join(loc_list)},{fl_df.state}"
                        p_text += f"\t{start_link}|*{fl_df.state}*: pval={round(fl_df.val, 2)}>\n"
                    if (i+1)%50==0:
                        logger.info(event,
                                    contd = (i+1)//50,
                                    payload=p_text,
                                    hits=flags.shape[0])
                        p_text = ""

def save_files(loc_list, files_dict, flag_meta, logger):
    """
    Save files in the proper location.

    Loc list is a list of parameters useful for creating output dictionaries.
    Files dict is a dictionary of file name and their corresponding dataframe.

    Offline parameter shows where to save the data.
    """
    log_flag(files_dict, loc_list, logger)
    loc = loc_list[0]
    if len(loc_list) > 1:
        loc = '/'.join(loc_list[:-1])
    if not flag_meta['remote']:
        _ = [os.makedirs(loc + '/' + '/'.join(x.split('/')[:-1]), exist_ok=True) for x
            in list(files_dict.keys()) if
            not os.path.exists(x)]
        for (x, y) in files_dict.items():
            y.to_csv(f'{loc}/{x}')
    else:
        session = boto3.Session(
            aws_access_key_id=flag_meta["aws_access_key_id"],
            aws_secret_access_key=flag_meta["aws_secret_access_key"])
        s3 = session.resource('s3')
        for key, df in files_dict.items():
            s3.Object('delphi-covidcast-public',
                      f"flags-dev/{loc}/{key}").put(
                Body=df.to_csv(), ACL='public-read')

def flagger_df(raw_df, flag_p, flag_meta, logger):
    """
    Run the flagger on an already existing raw dataframe.

    This method recreates all files associated with the flagger including:
    - All reference dataframes
    - AR method dataframes
    - All flags

    It returns nothing.
    """
    files_dict = {}
    for lag in pd.unique(raw_df['lag']):
        proc_df = raw_df.query('lag==@lag').drop(columns=['lag'])#.set_index('state').T
        if not proc_df.empty:
            proc_df.index = pd.to_datetime(proc_df.index)
            df_dict = gen_ref_dfs(proc_df, logger)
            f_list = [f"{flag_p['sig_fold']}",
                      f"{flag_p['sig_str']}",
                      f"{flag_p['sig_type']}",
                      f'window_{lag}',
                      f'train_{flag_meta["n_train"]}_lags_{flag_meta["ar_lags"]}']
            files_dict.update(df_dict)
            files_dict.update(gen_ar_files('/'.join(f_list[-1:]),
                   df_dict['ref_dfs/wkdy_corr.csv'],
                   flag_meta["ar_lags"], flag_meta["n_train"],
                   pd.date_range(pd.to_datetime(flag_p["resid_start_date"]),
                                 pd.to_datetime(flag_p["resid_end_date"])),
                   pd.date_range(pd.to_datetime(flag_p["eval_start_date"]),
                                 pd.to_datetime(flag_p["eval_end_date"]))))
            if not flag_meta['remote']:
                f_list[0] = f"{flag_meta['output_dir']}/{f_list[0]}"
            save_files(f_list, files_dict, flag_meta, logger)

def flagger_io(df, flag_p, flag_meta, logger):
    """
    Run the flagger on an already existing raw dataframe.

    This method recreates only missing files, which usually means not regenerating the ref files.

    It returns nothing.
    """
    f_list = [f"{flag_p['sig_fold']}",
              f"{flag_p['sig_str']}",
              f"{flag_p['sig_type']}",
              f'train_{flag_meta["n_train"]}_lags_{flag_meta["ar_lags"]}']

    if flag_meta['remote']:
        gen_dict, gen_ar_dict = missing_remote_files(df, flag_p, flag_meta)
        files_dict = gen_files(gen_dict, gen_ar_dict, logger)
        save_files(f_list, files_dict, flag_meta, logger)
    else:
        gen_dict, gen_ar_dict = missing_local_files(df, flag_p, flag_meta)
        files_dict = gen_files(gen_dict, gen_ar_dict, logger)
        f_list[0] = f"{flag_meta['output_dir']}/{f_list[0]}"
        save_files(f_list, files_dict, flag_meta, logger)

#Functions for Calling Code

def rel_files_table(input_dir, start_date, end_date, sig_str):
    """Determine which files we need to process & missing files.

    This format helps

    This can be for the calling code.
    """
    dates_range = pd.date_range(start_date, end_date)
    rel_files = pd.DataFrame()
    rel_files['fname'] = glob.glob(f'{input_dir}/*{sig_str}*')
    rel_files['fname'] = rel_files['fname'].astype(str)
    rel_files['fdate'] = pd.to_datetime(
        rel_files['fname'].str.rsplit('/', n=1, expand=True)[1].
            str.split('_', n=1, expand=True)[0],
        format='%Y%m%d', errors='coerce')
    rel_files = rel_files.set_index('fdate')
    merge_files = pd.DataFrame(index=dates_range)
    rel_files = merge_files.merge(rel_files.sort_index(), how='left', left_index=True,
                                  right_index=True).fillna(method='ffill')
    rel_files['win_sub'] = list(rel_files.reset_index(drop=True).groupby(['fname']).cumcount())
    return rel_files

def params_meta(params):
    """Create updated parameters for continuous use."""
    today = date.today()
    lags = [x if x != 'var' else 60 for x in params['lags']]
    ar_train = params['n_train']
    ar_lags = params['ar_lags']
    resid_dist = 100
    eval_dist = 1
    e_date = today-timedelta(min(lags))
    s_date = today - timedelta(resid_dist+eval_dist+ar_train-ar_lags) - timedelta(max(lags))
    params["df_start_date"] = s_date.strftime("%m/%d/%Y")
    params["df_end_date"] = e_date.strftime("%m/%d/%Y")

    r1_date = s_date + timedelta(ar_train+ar_lags)
    params["resid_start_date"] = r1_date.strftime("%m/%d/%Y")
    r2_date = s_date + timedelta(ar_train+ar_lags+resid_dist)
    params["resid_end_date"] = r2_date.strftime("%m/%d/%Y")

    es_date = e_date -timedelta(eval_dist)
    params["eval_start_date"] = es_date.strftime("%m/%d/%Y")
    params["eval_end_date"] = e_date.strftime("%m/%d/%Y")
    return params

def raw_df_from_api(flag_p):
    """Make raw dataframe from covidcast api given parameters."""
    start_d = pd.to_datetime(flag_p['df_start_date'])
    end_d = pd.to_datetime(flag_p['df_end_date'])
    sig = flag_p['sig_str']
    source = flag_p['sig_fold']
    all_lags = pd.DataFrame()
    cols = ['ak', 'al', 'ar', 'as', 'az', 'ca', 'co', 'ct', 'dc', 'de', 'fl', 'ga',
            'gu', 'hi', 'ia', 'id', 'il', 'in', 'ks', 'ky', 'la',
            'ma', 'md', 'me', 'mi', 'mn', 'mo', 'mp', 'ms', 'mt', 'nc',
            'nd', 'ne', 'nh', 'nj', 'nm', 'nv', 'ny', 'oh', 'ok',
            'or', 'pa', 'pr', 'ri', 'sc', 'sd', 'tn', 'tx', 'ut', 'va', 'vi', 'vt',
            'wa', 'wi', 'wv', 'wy'
            ]
    for lag in flag_p['lags']:
        tot_df = pd.DataFrame()
        for date_s in pd.date_range(start_d, end_d):
            if lag == 'var':
                data = covidcast.signal(source, sig, date_s, date_s,
                                        "state")
            else:
                data = covidcast.signal(source, sig, date_s, date_s,
                                    "state", as_of=date_s-timedelta(lag))
            if data is not None:
                data = data[['geo_value', 'value', 'time_value']]
                data.columns=['state', 'value', 'index']
            else:
                data = pd.DataFrame()
                data['state'] = cols
                data['index'] = date_s
                data['value'] = None
            tot_df = pd.concat([tot_df, data])
        if not tot_df.empty:
            tot_df = tot_df.set_index(["index", 'state'])
            tot_df = tot_df.unstack(level=0)
            tot_df.columns = tot_df.columns.droplevel()
            tot_df = tot_df.T
            tot_df['lag'] = lag
        all_lags = pd.concat([tot_df, all_lags], axis=0)
    if flag_p.get('raw_df', None) is None:
        all_lags.to_csv(flag_p['raw_df'])
    return all_lags

def flagging(params, df_list=None):
    """Organization method for different flagging options."""
    logger = get_structured_logger(
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))
    flag_meta = params['flagging_meta']
    for i, flag_p in enumerate(params['flagging']):
        df = None
        if df_list is not None:
            df = df_list[i]
        if df is None:
            try:
                df = pd.read_csv(flag_p['raw_df'], index_col=0, parse_dates=[0], header=[0])
            except FileNotFoundError:
                df = raw_df_from_api(flag_p)
        if flag_meta['flagger_type'] == 'flagger_io':
            flagger_io(df, flag_p, flag_meta, logger)
        else:
            flagger_df(df, flag_p, flag_meta, logger)
