import covidcast
import numpy as np

from ..validator.datafetcher import load_all_files
import pandas as pd
from ..weekday import Weekday.calc_adjustment
from scipy.stats import poisson, nbinom
from .. import (
    get_structured_logger,
)

def outlier(df, iqr_list2=None, replace=pd.DataFrame(), replace_use=False):
    df_fix_unstack = df.ffill()
    diff_df_small = df_fix_unstack.copy().diff(1).bfill()
    upper = 0.80
    lower = 0.20
    df['day'] = [x.weekday() for x in list(df.index)]
    diff_df2 = diff_df_small
    diff_df2['day'] = df['day']
    diff_df2_stack = diff_df2.drop(columns=['day']).stack().reset_index()
    diff_df2_stack.columns = ['date', 'state', 'val']
    diff_df2_stack['weekday'] = diff_df2_stack.date.dt.weekday
    if iqr_list2 is None:
        iqr_list2 = []
        iqr_spec_df2 = diff_df2_stack.iloc[1:, :]
        for i, (_, ldf) in enumerate(iqr_spec_df2.groupby(['weekday'])):
            iqr = ldf.groupby('state').apply(lambda x: x.val.quantile([lower, 0.5, upper]).T)
            def fix_iqr(x):
                upper = 0.80
                lower = 0.20
                if x[upper] == x[lower]:
                    x[upper] += 2
                    x[lower] -= 2
                if x[lower] >= 0:
                    x[lower] = -1
                if x[upper] <= 0:
                    x[upper] = 1
                return x
            iqr = iqr.apply(lambda x: fix_iqr(x), axis=1)
            iqr['delta'] = 1.5 * (np.ceil(iqr[upper]) - np.floor(iqr[lower]))
            iqr['lower_bound'] = iqr[lower] - iqr['delta']
            iqr['upper_bound'] = iqr[upper] + iqr['delta']
            iqr.columns = iqr.columns.astype(str)
            iqr_list2.append(iqr)
    p2_outliers = []
    for i, row in diff_df2_stack.iterrows():
        if replace_use:
            if not replace.empty:
                iqr_df2 = iqr_list2[row.weekday]
                row.state = df.columns[0]
                if not replace.query("date==@row.date and state==@row.state").empty:
                    yesterday_date = row.date - pd.Timedelta('1d')
                    if (yesterday_date in df_fix_unstack.index):
                        f = float(df_fix_unstack.loc[yesterday_date, row.state] + (1 + iqr_df2.loc[row.state, '0.5']))
                        df_fix_unstack.loc[row.date, row.state] = max(f, 1.0)
                        p2_outliers.append(row)
        else:
            iqr_df2 = iqr_list2[row.weekday]
            if (not (iqr_df2.loc[row.state, 'upper_bound'] >= diff_df2.loc[row.date, row.state] >= iqr_df2.loc[
                row.state, 'lower_bound'])):
                yesterday_date = row.date - pd.Timedelta('1d')
                if (yesterday_date in df_fix_unstack.index):
                    f = float(df_fix_unstack.loc[yesterday_date, row.state] + (1 + iqr_df2.loc[row.state, '0.5']))
                    df_fix_unstack.loc[row.date, row.state] = max(f, 1.0)
                    p2_outliers.append(row)
    return df_fix_unstack, iqr_list2, p2_outliers

def spike_outliers(df):
    def outlier_nearby(frame):
        size_cut, sig_cut, sig_consec = 10, 3, 2.25
        if (not pd.isna(frame['ststat'])) and (frame['ststat'] > sig_consec):
            return True
        if pd.isna(frame['ststat']) and (frame['ftstat'] > sig_consec):
            return True
        return False
    def outlier_flag(frame):
        size_cut, sig_cut, sig_consec = 5, 3, 2.25
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

    window_size = 28
    all_frames = df
    size_cut, sig_cut, sig_consec = 10, 3, 2.25
    shift_val = -1 if window_size % 2 == 0 else 0
    all_full_frames = []
    for col in all_frames.columns:
    group = all_frames[col].to_frame()
    group.columns =  ["value"]
    rolling_windows = group["value"].rolling(
        window_size, min_periods=window_size)
    center_windows = group["value"].rolling(
        window_size, min_periods=window_size, center=True)
    fmedian = rolling_windows.median()
    smedian = center_windows.median().shift(shift_val)
    fsd = rolling_windows.std() + 0.00001  # if std is 0
    ssd = center_windows.std().shift(shift_val) + 0.00001  # if std is 0
    group['ftstat'] = abs(group["value"] - fmedian.fillna(0)) / fsd
    group['ststat'] = abs(group["value"] - smedian.fillna(0)) / ssd
    group['state'] = col
    all_full_frames.append(group)
    all_frames = pd.concat(all_full_frames)

    outlier_df = all_frames.reset_index().sort_values(by=['state', 'date']) \
        .reset_index(drop=True).copy()
    outliers = outlier_df[outlier_df.apply(outlier_flag, axis=1)]
    outliers_reset = outliers.copy().reset_index(drop=True)
    upper_index = list(filter(lambda x: x < outlier_df.shape[0],
                            list(outliers.index+1)))
    upper_df = outlier_df.iloc[upper_index, :].reset_index(drop=True)
    upper_compare = outliers_reset[:len(upper_index)]
    sel_upper_df = upper_df[upper_compare["state"]
                          == upper_df["state"]].copy()
    lower_index = list(filter(lambda x: x >= 0, list(outliers.index-1)))
    lower_df = outlier_df.iloc[lower_index, :].reset_index(drop=True)
    if lower_df.empty:
      lower_compare = outliers_reset[0:0]
    else:
      lower_compare = outliers_reset[-len(lower_index):].reset_index(drop=True)
    sel_lower_df = lower_df[lower_compare["state"]
                          == lower_df["state"]].copy()
    outliers_list = [outliers]
    if sel_upper_df.size > 0:
      outliers_list.append(
          sel_upper_df[sel_upper_df.apply(outlier_nearby, axis=1)])
    if sel_lower_df.size > 0:
      outliers_list.append(
          sel_lower_df[sel_lower_df.apply(outlier_nearby, axis=1)])

    all_outliers = pd.concat(outliers_list). \
      sort_values(by=['date', 'state']). \
      drop_duplicates().reset_index(drop=True)
    return all_outliers

def predict_val(col, params_state, lags_names):
    state_df = pd.DataFrame()
    for i in range(1, 8):
        state_df[f'lags_{i}'] = state_df['model'].shift(i)
    state_df = state_df.dropna()
    x = state_df.drop(columns=['model'])
    x = x[lags_names]
    beta = np.asarray(params_state[col.name])
    pred_val = pd.Series(np.dot(x, beta), index=state_df.index)
    return pred_val

def f(x, y, model=""):
    if model == "nbinom":
      alpha=0.5
      n = 1/alpha
      p = 1/(1+(alpha*y))
      return nbinom.cdf(x, n, p) #nbinom.cdf(x, y, 0.5)
    else:
      return poisson.cdf(x,y)
vecF = np.vectorize(f)            # vectorize the function with numpy.vectorize
def bin(ref_y, ref_y_predict):
  return vecF(np.log(1 + ref_y), np.log(1 + ref_y_predict), "nbinom")

def return_vals(val, ref_dict_met):
  val_dict_items = {}
  cats = ['bin']
  cat_fns = [bin]
  for key, ref_dict in ref_dict_met.items():
    p_val = []
    states_flagged = {}
    special_states = pd.Series(dtype=float)
    ref_y = val['y'].clip(1)
    ref_y_predict = val['y_pred'].clip(1)
    for i, cat in enumerate(cats):
        cat_fn = cat_fns[i]
        dist = pd.Series(cat_fn(ref_y, ref_y_predict).T, index=val.index, dtype=float)
        val[f'{cat}'] = dist.copy()
        pval = dist.copy()
        for state in dist.index:
          if key==1:
            ref_dist = ref_dict[cat][state]
          elif key==3:
            ref_dist = pd.DataFrame(ref_dict[cat]).to_numpy().flatten()
          else:
            ref_dist = pd.Series(cat_fn(val['y'],val['y_pred']), index=val['y'].index).T.drop([state])
            if cat != 'resid':
              ref_dist = ref_dist#.replace(0, np.nan).replace(1, np.nan)
          pval[state] = (sum(ref_dist  < dist[state])/ len(ref_dist ))
        #pval.loc[special_states] = np.nan
        val[f'{cat}_r'] = pval
        val[f'{cat}'] = dist
    val_dict_items[key] = val.copy()
  return val_dict_items, val

def eval_day(input_df, iqr_dict, weekend_params, weekday_params, linear_coeff):
    val = pd.DataFrame()
    val['y_raw'] = input_df.iloc[-1, :]
    as_of = input_df.index[-1]
    lags_names = [f'lags_{i}' for i in range(7)]
    input_df, _, flag_out = outlier(input_df, iqr_list2=iqr_dict['Before']).clip(0)
    input_df= calc_adjustment(weekday_params
                    ,input_df.copy().reset_index(), list(input_df.columns),
                    'date').fillna(0).set_index('date')
    input_df,  _, flag_out1= outlier(input_df, iqr_list2=iqr_dict['After'], replace=spike_outliers(input_df), replace_use=True)
    y_predict = input_df.iloc[:, : ].apply(predict_val,params_state=linear_coeff, lags_names=lags_names, axis=0).T.clip(0)
    val['y_pred'] = y_predict
    val['y'] = y
    flag_out['flag'] = 'weekday outlier'
    flag_out1['flag'] = 'large_spikes'
    return input_df, val, pd.concat([flag_out, flag_out1], axis=1).query("date==@as_of")[['state', 'pval', 'flag']]

def flash_eval_lag(input_df, range_tup, lag, signal, logger):
    """Create a list of the most interesting points to return to a user that is saved in the output dictionary
    Inputs: - input_df = A dataframe with the past 7 days of data  at the same lag including most recent data as final row.
            This is created from the files in the cache (else they are pulled from the API)
            - lag: which lag are we working on
            - range_tup: acceptable range of values
    Other Files:
            - dist_min = Min EVD distribution for comparison
            - dist_max = Max EVD distribution for comparison
            - iqr_dictionary = Range values for removing large spikes before & after weekday corrections
            - weekend_params = Saturday and Sunday manual corrections for weekday
            - weekday_params: Weekday correction parameters
            - linear_coeff: Linear regression coeffecients
            - range_tup: Acceptable range of values for that signal
            These are created from files in the reference folder.
    Output: None
    """


    ref_date = input_df.index.iloc[-1]
    report_date = ref_date + pd.Timedelta(f'{lag}d')


    #Get necessary reference files per signal
    dist_min = pd.read_csv(f"reference/{signal}/dist_min.csv")
    dist_max = pd.read_csv(f"reference/{signal}/dist_max.csv")
    iqr_dict = {'Before': pd.read_csv(f"reference/{signal}/iqr_dict0.csv"),
                'After': pd.read_csv(f"reference/{signal}/iqr_dict0.csv")}
    weekend_params = pd.read_csv(f"reference/{signal}/weekend.csv")
    weekday_params = pd.read_csv(f"reference/{signal}/weekday.csv")
    linear_coeff = pd.read_csv(f"reference/{signal}/linear_coeff.csv")


    #Make corrections & predictions
    input_df, raw_val, preprocess_outlier = eval_day(input_df, iqr_dict, weekend_params, weekday_params, linear_coeff)
    out_range_outlier = list(raw_val['y_raw'][raw_val['y_raw'] < range_tup[0]].index) + list(raw_val['y_raw'][raw_val['y_raw'] < range_tup[-1]].index)

    #Anomaly Detection
    thresh = 0.01
    val_min = return_vals(raw_val, dist_min)[f"bin_r"].to_frame()
    val_max = return_vals(raw_val, dist_max)[f"bin_r"].to_frame()
    val_min['flags'] = 'EVD_min'
    val_max['flags'] = 'EVD_max'
    val_min.columns = ['pval', 'flags']
    val_max.columns = ['pval', 'flags']
    def process_anomalies(y, t_skew=None):
        def standardize(y, t_skew=None):
            val = y.pval
            if t_skew == None:
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
        tmp_list = y.apply(lambda z: standardize(z, t_skew=t_skew), axis=1)
        y['pval'] = tmp_list
        if not y.empty:
            y = y[['pval', 'state']]
        return y.reset_index(drop=True)

    min_thresh = thresh * 2
    max_thresh = 1 - (thresh * 2)
    max_anomalies = process_anomalies(val_max, 'max').dropna(axis=1).query('pval>@max_thresh')
    min_anomalies = process_anomalies(val_min, 'min').dropna(axis=1).query('pval<@min_thresh')


    total_flags = 0
    for (name, df) in zip([out_range_outlier, preprocess_outlier, max_anomalies, min_anomalies],
                          ['out_of_range', 'large_spike or weekday', 'max_anomalies', 'min_anomalies'])
        p_text = ""
        p_text += f"*{ref_date}* \n"
        for j, row in df.iterrows():
            total_flags += 1
            start_link = f"{HTML_LINK},{ref_date},{report_date},{row.state}"
            p_text += f"\t{start_link}|*{fl_df.state}*>\n"

        if (total_flags + 1) % 50 == 0:
            logger.info(name,
                        contd=(i + 1) // 50,
                        payload=p_text,
                        hits=flags.shape[0])
            p_text = ""

    return

def flash_eval(files_list, params):
    #get files from the cache, need the last 14 days (7 days) (as of)
    # if they aren't there, then regenerate them

    #STEP 1: API Call for file creation for prior days

    most_recent_d = params["validation"]["common"]["end_date"]
    source = params["validation"]["common"]["data_source"]
    #get most recent d from current files in cache
    #filter those by value & if they are in the json params and then generate the API files
    file_tup = load_all_files(params["validation"]["common"]["export_dir"], most_recent_d-pd.Timedelta('14d'), most_recent_d)
    available_signals =  ["_".join(x.split("_")[2:]) for (x, y, z) in file_tup] #need the data anyway to ad din
    signals = list(set(available_signals) & set(params["flash"]["signals"]))
    for signal in signals:
        curr_df = pd.DataFrame()
        for date_s in pd.date_range(most_recent_d-pd.Timedelta('14d'), most_recent_d-pd.Timedelta('1')):
            data = covidcast.signal(source, signal, date_s - pd.Timedelta(f'7d'), date_s,
                                    geo_type="nation", as_of=date_s)
            data2 = covidcast.signal(source, signal, date_s - pd.Timedelta(f'7d'), date_s,
                                     geo_type="state", as_of=date_s)
            data3 = covidcast.signal(source, signal, date_s - pd.Timedelta(f'7d'), date_s,
                                     geo_type="county", as_of=date_s)
            if (data is not None) or ((data2 is not None) or (data3 is not None)):
                data = pd.concat([data, data2, data3])
                if data is not None:
                    data = data[['geo_value', 'value', 'time_value']]
                    data.columns = ['state', 'value', 'ref']
                    data['as_of'] = date_s
                    data['lag'] = data['ref'] - data['as_of']
                    data = data.set_index(['state', 'lag', 'ref', 'as_of'])
                    curr_df = pd.concat([data, curr_df])
        #Add in data gathered today
        for (filename, _ , data) in file_tup:
            if signal in filename:
                as_of = most_recent_d
                ref = filename.split('_')[0]
                region = filename.split('_')[1]
                if region in ['county', 'state', 'nation']:
                    data = data[['geo_value', 'value']]
                    data.columns = ['state', 'value']
                    data['as_of'] = as_of
                    data['ref'] = ref
                    data['lag'] = data['ref'] - data['as_of']
                    data = data.set_index(['state', 'lag', 'ref', 'as_of'])
                    curr_df = pd.concat([data, curr_df])
        curr_df = curr_df[curr_df.index.drop_duplicates()].reset_index()
        for lag in range(7):
            #The input df for evaluation is today and the past 7 days at a particular lag
            date_range = pd.date_range(most_recent_d-pd.Timedelta(f'{lag+7}d'), most_recent_d-pd.Timedelta(f'{lag}d'))
            input_df = curr_df.query('lag=@lag and ref in @date_range').sort_values('ref')
            date_df = pd.DataFrame(columns=input_df.columns)
            date_df['ref'] = date_range
            date_df = date_df.set_index('ref')
            input_df = input_df.set_index('ref')
            input_df = input_df.merge(date_df, left_on_index=True, right_on_index=True, how='right').ffill().bfill()
            flash_eval_lag(input_df, lag, signal)