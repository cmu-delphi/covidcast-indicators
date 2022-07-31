"""Functions used to generate files related to the AR process."""
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

def ar_results(df: pd.DataFrame, ar_lag, n_train, resid_dist_dates, eval_dates):
    """We want to create the results from using an AR forecaster given params.

    The input is the final adjusted dataframe and the number of lags the AR forecaster
    considers/the number of samples to train on.

    We repeat this process separately for determining the residual distribution and for the
    dates that we are evaluating the flagging system on.

    The output is the values from the AR forecaster in the same format as the input dataframe.
    """
    assert n_train > 1, 'Number of samples to train on must be at least 2.'
    assert ar_lag > 0, 'Need at least 1 lag for the AR model.'

    lags_names = []
    for i in range(ar_lag):
        lags_names.append(f'lags_{i+1}')
    model_str = 'model ~ ' + '+'.join(lags_names)

    def create_residuals(df, curr_day_list, ar_lag, n_train, model_str):
        all_days_pred = []
        df = df.reset_index()
        for curr_day in curr_day_list:
            start_train = curr_day - pd.Timedelta(days=n_train+ar_lag)
            assert start_train > df.date.min(),\
                "Necessary start date for AR parameters is before that provided in the dataframe."
            all_tmp = df[start_train <= df.date]
            all_tmp = all_tmp[all_tmp.date <= curr_day]
            all_tmp = all_tmp.set_index(['date'])
            def pred_val(col):
                state_df = pd.DataFrame()
                state_df['model'] = col
                for i in range(1, ar_lag + 1):
                    state_df[f'lags_{i}'] = state_df['model'].shift(i)
                state_df = state_df.dropna()
                res_ols = smf.ols(model_str, state_df.iloc[:-2, :]).fit()
                y_t_pred = np.array(res_ols.predict(state_df.iloc[-2, :]))
                return y_t_pred[0]

            y_t_state = all_tmp.apply(pred_val, axis=0, result_type='reduce')
            all_days_pred.append(y_t_state)
        ret_df = pd.concat(all_days_pred, axis=1)
        ret_df.columns = curr_day_list
        return ret_df
    resid_r = create_residuals(df, resid_dist_dates, ar_lag, n_train, model_str)
    eval_r = create_residuals(df, eval_dates, ar_lag, n_train, model_str)
    ret_df = pd.concat([eval_r, resid_r], axis=1).T
    return ret_df


def calculate_report_flags(dist_df, dist_range, eval_range, thresh=0.025):
    """Create residual distribution to flagging points in evaluation set that are > threshold.

    The input is the residual dataframe, ranges for determining the residual distribution & eval set
    and the threshold to flagging values.
    """
    hist_data = dist_df[dist_df.index.isin(dist_range)].stack().fillna(0)
    eval_data = dist_df[dist_df.index.isin(eval_range)].applymap(lambda x:
                        (sum(hist_data < x) / len(hist_data)))
    g = eval_data.stack().reset_index()
    g.columns = ['date', 'state', 'val']
    ret_series = pd.concat([g[g.val < thresh], g[g.val < thresh]]).sort_values(by=['val'])
    return ret_series


def gen_ar_files(key, input_df, ar_lag, n_train, resid_dist_dates, eval_dates):
    """Create AR files given relevant parameters."""
    ar_df = ar_results(input_df, ar_lag, n_train, resid_dist_dates, eval_dates)
    ar_flags = calculate_report_flags(input_df-ar_df, resid_dist_dates, eval_dates)
    return {f'{key}/ar_output.csv': ar_df,
            f'{key}/flag_ar.csv': ar_flags}
