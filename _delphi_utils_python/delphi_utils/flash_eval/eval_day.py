"Functions pretaining to running FlaSH daily."
import time
import math
import numpy as np
import covidcast
from scipy.stats import nbinom
import pandas as pd
from ..validator.datafetcher import load_all_files, read_filenames
from ..weekday import Weekday
from .. import (
    get_structured_logger,
)
from .constants import HTML_LINK


def fix_iqr(x):
    "Changes to account that the delta may be positive or negative."
    upper = 0.75
    lower = 0.25
    if x[upper] == x[lower]:
        x[upper] += 2
        x[lower] -= 2
    if x[lower] >= 0:
        x[lower] = -1
    if x[upper] <= 0:
        x[upper] = 1
    return x

def outlier(df, iqr_list=None, replace=pd.DataFrame(), replace_use=False):
    """Two diferent outlier processing methods.
    One, using the interquartile-range, and the other using the baseline spikes method.
    Input: df:streaming dataframe
    IQR_list: If available, provides the IQR needed for determining weekday outliers
    Replace: Points to replace (if they were global outliers)
    Replace_use: Distinguishes between the two times of outliers: global and weekday
    """
    df_fix_unstack = df.ffill()
    diff_df_small = df_fix_unstack.copy().diff(1).bfill()
    upper = 0.75
    lower = 0.25
    df['day'] = [x.weekday() for x in list(df.index)]
    diff_df2 = diff_df_small
    diff_df2['day'] = df['day']

    diff_df2_stack = diff_df2.drop(columns=['day']).stack().reset_index()
    diff_df2_stack.columns = ['date', 'state', 'val']
    diff_df2_stack['weekday'] = diff_df2_stack.date.dt.weekday

    if df.columns.nlevels > 1:
        diff_df2.columns = diff_df2.columns.droplevel()
        df_fix_unstack.columns = df_fix_unstack.columns.droplevel()
    if iqr_list is None:
        iqr_list = []
        iqr_spec_df2 = diff_df2_stack.iloc[1:, :]
        for _, (_, ldf) in enumerate(iqr_spec_df2.groupby(['weekday'])):
            iqr = ldf.groupby('state').apply(lambda x: x.val.quantile([lower, 0.5, upper]).T)
            iqr = fix_iqr(iqr)
            iqr['delta'] = 1.5 * (np.ceil(iqr[upper]) - np.floor(iqr[lower]))
            iqr['lower_bound'] = iqr[lower] - iqr['delta']
            iqr['upper_bound'] = iqr[upper] + iqr['delta']
            iqr.columns = iqr.columns.astype(str)
            iqr_list.append(iqr)
    p2_outliers = []
    def eval_row(row, replace_use, iqr_list2, replace, df_fix_unstack, diff_df2):
        if replace_use:
            if not replace.empty:
                iqr_df2 = iqr_list2[row.weekday]
                row.state = df.columns[0]
                if not replace.query("date==@row.date and state==@row.state").empty:
                    yesterday_date = row.date - pd.Timedelta('1d')
                    if yesterday_date in df_fix_unstack.index:
                        f = float(df_fix_unstack.loc[yesterday_date, row.state] +
                                  (1 + iqr_df2.loc[row.state, '0.5']))
                        df_fix_unstack.loc[row.date, row.state] = max(f, 1.0)
                        p2_outliers.append(row.copy())
        else:
            iqr_df2 = iqr_list2[row.weekday]
            iqr_df2 = iqr_df2.set_index('state')
            if (not (iqr_df2.loc[row.state, 'upper_bound'] >=
                     diff_df2.loc[row.date, row.state] >= iqr_df2.loc[row.state, 'lower_bound'])):
                yesterday_date = row.date - pd.Timedelta('1d')
                if yesterday_date in df_fix_unstack.index:
                    f = float(df_fix_unstack.loc[yesterday_date, row.state]
                              + (1 + iqr_df2.loc[row.state, '0.5']))
                    df_fix_unstack.loc[row.date, row.state] = max(f, 1.0)
                    p2_outliers.append(row.copy())
    diff_df2_stack.apply(lambda x:eval_row(x, replace_use, iqr_list,
                                           replace, df_fix_unstack, diff_df2,) , axis=1)
    return df_fix_unstack, iqr_list, pd.DataFrame(p2_outliers)

def spike_outliers(df):
    """An adaptation of the existing spike outliers baseline.
    Input: df: input df with columns as geographies and time as the rows
    Output: global outliers dataframe"""
    size_cut, sig_cut = 10, 3

    def outlier_flag(frame):
        if (abs(frame["value"]) > size_cut) and not (pd.isna(frame["ststat"])) \
                and (frame["ststat"] > sig_cut):
            return True
        if (abs(frame["value"]) > size_cut) and (pd.isna(frame["ststat"])) and \
                not (pd.isna(frame["ftstat"])) and (frame["ftstat"] > sig_cut):
            return True
        if (frame["value"] < -size_cut) and not (pd.isna(frame["ststat"])) and \
                not pd.isna(frame["ftstat"]):
            return True
        return False

    outliers_list = []
    group_list = []
    all_frames_orig = df.copy()
    def spike(x):
        window_size = 7
        shift_val = -1 if window_size % 2 == 0 else 0
        group = x.to_frame()
        group.columns =  ["value"]
        rolling_windows = group["value"].rolling(
            window_size, min_periods=window_size)
        center_windows = group["value"].rolling(
            window_size, min_periods=window_size, center=True)
        fmedian = rolling_windows.median()
        smedian = center_windows.median().shift(shift_val)
        fsd = rolling_windows.std() + 0.000001
        ssd = center_windows.std().shift(shift_val) + 0.000001
        group['ftstat'] = abs(group["value"] - fmedian.fillna(0)) / fsd
        group['ststat'] = abs(group["value"] - smedian.fillna(0)) / ssd
        group['state'] = x.name
        group_list.append(group)

    spike(all_frames_orig.T)
    all_frames = pd.concat(group_list)
    outlier_df = all_frames.reset_index().sort_values(by=['state', 'ref']) \
        .reset_index(drop=True).copy()
    outliers = outlier_df[outlier_df.apply(outlier_flag, axis=1)]

    outliers_list.append(outliers)

    all_outliers = pd.concat(outliers_list).sort_values(by=['ref', 'state']). \
        drop_duplicates().reset_index(drop=True)
    return all_outliers

#Empirically validated test-statistic distribution
def bin_dist(ref_y, ref_y_predict):
    "A vectorized test statistic distribution [log]."
    def ts_dist(x, y, model=""):
        "Initial test statistic distribution which is then vectorized."
        if model == "nbinom":
            alpha = 0.5
            n = 1 / alpha
            p = 1 / (1 + (alpha * y))
        return nbinom.cdf(x, n, p)
    vec_ts_dist = np.vectorize(ts_dist)
    return vec_ts_dist(np.log(1 + ref_y), np.log(1 + ref_y_predict), "nbinom")

def eval_day(input_df, iqr_dict, ref_date, weekday_params, linear_coeff):
    """Submethod to correct historical data and predict if today's data is a flag.
    Input: input_df: a dataframe of the past 7 days.
    iqr_dict: Dictionary of the inter-quartile ranges acceptable when conducting outlier analysis
    ref_date: The reference date of the most recent dataa
    weekday params: Parameters for the weekday correction
    linear_coeff: The linear coeffecients for predicting today's indicator
    value (to compare to prior historical values)

    Output: origini
    """
    y_values_df = pd.DataFrame()
    y_values_df['y_raw'] = input_df.iloc[-1, :]
    # remove out-of-range data & add 1 to address multiplicative issues in weekday smoothing
    corrected_input_df = input_df.clip(0)+1
    lags_names = [f'lags_{i}' for i in range(1, 8)]
    #first pass of outlier detection to detect and correct weekday outliers
    corrected_input_df, _, weekday_outlier_flags = outlier(corrected_input_df,
                                                           iqr_list=iqr_dict['Before'])
    corrected_input_df = corrected_input_df.clip(0) #for corrections that are <0
    #apply the weekday correction
    corrected_input_df= Weekday.calc_adjustment(weekday_params.to_numpy()
                    ,corrected_input_df.copy().reset_index(), list(corrected_input_df.columns),
                    'ref').fillna(0).set_index('ref')
    #second pass outlier detection to detect and correct global outliers
    corrected_input_df,  _, large_spike_flags = outlier(corrected_input_df,
                iqr_list=iqr_dict['After'], replace=spike_outliers(input_df), replace_use=True)

    #final, prediction of the day's values
    def predict_val(col, params_state, lags_names):
        state_df = pd.DataFrame()
        state_df['model'] = col
        for i in range(1, 8):
            state_df[f'lags_{i}'] = state_df['model'].shift(i)
        state_df = state_df.dropna()
        x = state_df.drop(columns=['model'])
        x = x[lags_names]
        beta = np.asarray(params_state[col.name])
        pred_val = pd.Series(np.dot(x, beta), index=state_df.index)
        return pred_val
    y_predict = corrected_input_df.iloc[:, : ].apply(predict_val,
                        params_state=linear_coeff,
                        lags_names=lags_names, axis=0).T.clip(0)
    y_predict.columns = ['y_predict']
    y_values_df = y_values_df.merge(y_predict, left_index=True,
                        right_index=True, how='outer').droplevel(level=0)
    weekday_outlier_flags['flag'] = 'weekday outlier'
    large_spike_flags['flag'] = 'large_spikes'
    flags_returned = pd.concat([weekday_outlier_flags,
                        large_spike_flags], axis=0)
    flags_returned = flags_returned[flags_returned.date == ref_date]
    return y_values_df, flags_returned

def return_vals(val, ref_dist):
    """Returns the p-value of the test"""
    ref_y = val['y_raw'].clip(1)
    ref_y_predict = val['y_predict'].clip(1)
    dist = pd.Series(bin_dist(ref_y, ref_y_predict).T, index=val.index, dtype=float)
    dist.name = 'dist'
    pval = dist.copy()
    pval.name = 'pval'
    for state in dist.index:
        pval[state] = (sum(ref_dist.astype(float) < float(dist[state])) / len(ref_dist))
    val = val.merge(dist, left_on='state', right_index=True, how='outer')
    val = val.merge(pval, left_on='state', right_index=True, how='outer')
    return val

def process_anomalies(y, t_skew=None):
    "Create a meaningful outlier score."
    def standardize(y, t_skew=None):
        val = y.pval
        if t_skew is None:
            val = 2 * abs(y.pval - 0.5)
            if y.pval <= 0.5:
                val = 2 * abs(y.pval - 0.5)
        else:
            if t_skew == 'max':
                if y.pval < 0.5:
                    val = 0.5
            else:
                if y.pval > 0.5:
                    val = 0.5
        return val
    tmp_list = y.copy().apply(lambda z: standardize(z, t_skew=t_skew), axis=1)
    y['pval'] = tmp_list
    if not y.empty:
        y = y[['pval']]
    return y

def flash_eval_lag(input_df, range_tup, lag, signal, logger):
    """Create a list of the most interesting points that is saved in the output log
    Inputs: - input_df = A dataframe with the past 7 days of data
                at the same lag including most recent data as final row.
            This is created from the files in the cache (else they are pulled from the API)
            - lag: which lag are we working on
            - range_tup: acceptable range of values
    Other Files:
            - dist_min = Min EVD distribution for comparison
            - dist_max = Max EVD distribution for comparison
            - iqr_dictionary = Range values for removing large spikes before &
                                after weekday corrections
            - weekend_params = Saturday and Sunday manual corrections for weekday
            - weekday_params: Weekday correction parameters
            - linear_coeff: Linear regression coeffecients
            - range_tup: Acceptable range of values for that signal
            These are created from files in the reference folder.
    Output: None
    """


    ref_date = input_df.index[-1]
    report_date =ref_date + pd.Timedelta(f'{lag}d')

    #Get necessary reference files per signal
    dist_min = pd.read_csv(f"flash_ref/{signal}/dist_min.csv")['0']
    dist_max = pd.read_csv(f"flash_ref/{signal}/dist_max.csv")['0']
    bef_list = list(np.zeros(7))
    aft_list = list(np.zeros(7))
    for i in range(7):
        bef_list[i] = pd.read_csv(f"flash_ref/{signal}/iqr0_{lag}_{i}.csv")
        aft_list[i] = pd.read_csv(f"flash_ref/{signal}/iqr1_{lag}_{i}.csv")
    iqr_dict = {'Before': bef_list,
                'After': aft_list}
    weekday_params = pd.read_csv(f"flash_ref/{signal}/weekday_{lag}.csv", index_col=0)
    linear_coeff = pd.read_csv(f"flash_ref/{signal}/linear_coeff_{lag}.csv", index_col=0)


    # Make corrections & predictions
    y_values_df, preprocess_outlier = eval_day(input_df, iqr_dict,
                                               ref_date, weekday_params, linear_coeff)
    s_val = y_values_df['y_raw'].to_frame()
    out_range_outlier = pd.concat([s_val[s_val.y_raw < range_tup[0]],
                                   s_val[s_val.y_raw > range_tup[-1]]], axis=0)

    # Anomaly Detection
    thresh = 0.01

    val_min = return_vals(y_values_df, dist_min)[["pval"]]
    val_max = return_vals(y_values_df, dist_max)[["pval"]]
    val_min['flags'] = 'EVD_min'
    val_max['flags'] = 'EVD_max'
    val_min.columns = ['pval', 'flags']
    val_max.columns = ['pval', 'flags']


    min_thresh = thresh * 2
    max_thresh = 1 - (thresh * 2)
    max_anomalies = process_anomalies(val_max, 'max').dropna(
        axis=1)
    min_anomalies = process_anomalies(val_min, 'min').dropna(
        axis=1)
    max_anomalies = max_anomalies[max_anomalies.pval
                                  > max_thresh].sort_values('pval')#.reset_index(drop=True)
    min_anomalies = min_anomalies[min_anomalies.pval
                                  < min_thresh].sort_values('pval')#.reset_index(drop=True)

    starter_link = f"{HTML_LINK}{ref_date.strftime('%Y-%m_%d')},{report_date.strftime('%Y-%m_%d')}"

    total_flags = 0
    for (df, name) in zip(
            [out_range_outlier, preprocess_outlier, max_anomalies, min_anomalies],
            ['out_of_range', 'large_spike or weekday', 'max_anomalies', 'min_anomalies']):
        p_text = ""
        p_text += f"*{ref_date}* \n"
        iter_df = df.copy()
        if df.shape[0] > 20:
            iter_df = iter_df.iloc[:20, :]
        if iter_df.shape[0] > 0 :
            for _, row in iter_df.reset_index().iterrows():
                total_flags += 1

                start_link = f"{starter_link},{row.state}"
                if 'pval' in iter_df.columns :
                    p_text += f"\t{start_link}|*{row.state}, {row.pval}*>\n"
                elif 'y_raw' in iter_df.columns :
                    p_text += f"\t{start_link}|*{row.state}, {row.y_raw}*>\n"
            logger.info(name,
                        payload=p_text,
                        hits=iter_df.shape[0])
            p_text = ""

def flash_eval(params):
    """ Evaluate most recent data using FlaSH.
    First, get any necessary files from the cache or download from the API.
    Concat historical data with the most recent data from the cache.
    Then, evaluate today's data for up to lag 8 prior days,
            where each flag is written to the logger.
    Input: params
    Ouput: None
    """


    logger = get_structured_logger(
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))

    export_files = read_filenames(params["common"]["export_dir"])
    most_recent_d = pd.Series([
        pd.to_datetime(x.split('_')[0], format="%Y%m%d",errors='raise')
        if '.git' not in x  else None for (x, y) in export_files ]).dropna().max()
    source = params["validation"]["common"]["data_source"]
    file_tup = load_all_files(params["common"]["export_dir"],
                              most_recent_d-pd.Timedelta('14d'), most_recent_d)
    signals = params["flash"]["signals"]
    for signal in signals:
        curr_df = pd.DataFrame()
        start_time = time.time()
        for date_s in pd.date_range(most_recent_d-pd.Timedelta('14d'),
                                    most_recent_d-pd.Timedelta('1d')):
            data = covidcast.signal(source, signal,
                                    date_s - pd.Timedelta('7d'), date_s,
                                    geo_type="nation", as_of=date_s)
            data2 = covidcast.signal(source, signal,
                                     date_s - pd.Timedelta('7d'), date_s,
                                     geo_type="state", as_of=date_s)
            data3 = covidcast.signal(source, signal,
                                     date_s - pd.Timedelta('7d'), date_s,
                                     geo_type="county", as_of=date_s)
            if (data is not None) or ((data2 is not None) or (data3 is not None)):
                data = pd.concat([data, data2, data3])
                if data is not None:
                    data = data[['geo_value', 'value', 'time_value']]
                    data.columns = ['state', 'value', 'ref']
                    data['as_of'] = date_s
                    data['lag'] = (data['as_of'] - data['ref']).dt.days
                    data = data.set_index(['state', 'lag', 'ref', 'as_of'])
                    curr_df = pd.concat([data, curr_df])
        #Add in data gathered today
        for (filename, _ , data) in file_tup:
            if signal in filename:
                as_of = most_recent_d
                ref = pd.to_datetime(filename.split('_')[0],
                                     format="%Y%m%d",errors='raise')
                region = filename.split('_')[1]
                if region in ['county', 'state', 'nation']:
                    data = data[['geo_id', 'val']]
                    data.columns = ['state', 'value']
                    data['as_of'] = as_of #pd.to_datetime(pd.Timestamp.today())
                    data['ref'] = ref
                    data['lag'] = (data['as_of'] - data['ref']).dt.days
                    data = data.set_index(['state', 'lag', 'ref', 'as_of'])
                    curr_df = pd.concat([data, curr_df])
        curr_df = curr_df[~curr_df.index.duplicated(keep='first')].reset_index()
        end_time = time.time()
        print(f"Total Download Time: {start_time-end_time}")


        for lag in range(1,8):
            start_time = time.time()
            date_range = list(pd.date_range(most_recent_d-pd.Timedelta(f'{lag+7}d'),
                                            most_recent_d-pd.Timedelta(f'{lag}d')))
            input_df = curr_df.query('lag==@lag and ref in @date_range').sort_values('ref')
            date_df = pd.DataFrame()
            date_df['ref'] = date_range
            date_df = date_df.set_index('ref')
            input_df = input_df.set_index('ref')
            input_df = input_df.merge(date_df, left_index=True, right_index=True,
                                      how='right').ffill().bfill().reset_index()
            input_df = input_df.set_index(['ref', 'state'])[['value']].unstack().ffill().bfill()
            flash_eval_lag(input_df, [0, math.inf], lag, signal, logger)
            end_time = time.time()
            print(f"Time lag {lag}: {start_time - end_time}")
