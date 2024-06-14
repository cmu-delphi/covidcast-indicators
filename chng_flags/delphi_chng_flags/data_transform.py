"""Functions to raise flags, transform, and correct data."""
import numpy as np
import pandas as pd
import scipy.stats as stats
import statsmodels.formula.api as smf


def identify_correct_spikes(df: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
    """We want to find large spikes in data and then correct them.

    We do this by considering the difference between weekdays ex: Sun-Mon
    and identifying points that have a z-score > 3 for each of the weekday diffs (6 categories).

    Then, we correct those points by using the median of
    the weekday diff for that particular category in sequential order.

    Input: A raw dataframe

    Return: A dataframe with spikes corrected, flagged points list
    """
    #TO DO: Add in the header of this dataframe
    diff_df = df.drop(columns=['end', 'day']).diff(1).dropna()
    diff_df['day'] = df['day']
    medians = []
    for _, (_, ldf) in enumerate(diff_df.groupby(['day'])):
        medians.append(ldf.apply(lambda x: np.median(pd.Series(x).values), axis=0))
    diff_df = diff_df.drop(columns=['day'])
    points_out = diff_df.apply(lambda x: list(x[stats.zscore(x) > 3].index)
                                         + list(x[stats.zscore(x) < -3].index), axis=0)
    medians_df = pd.concat(medians, axis=1).T
    point_out_df = []
    for state, points_list in zip(list(points_out.index), points_out):
        point_out_df.append(df.loc[points_list, state])

        for po in points_list:
            day_week = df.loc[po, 'day']
            po2 = po - pd.Timedelta('1d')
            df.loc[po, state] = df.loc[po2, state] + medians_df.loc[day_week, state]
    return df, pd.DataFrame(point_out_df).stack()


def weekend_corr(df: pd.DataFrame, states: list) -> pd.DataFrame:
    """Use to correct the weekend volume.

    We correct the volume so that we can use the
    weekday effect correction which is a multiplicative method
    and performs poorly when the values for weekday counts are low.

    Input: Dataframe with large spikes correction
            List of states

    Returns: Dataframe with the weekends corrected
    """

    def create_wknum(df: pd.DataFrame) -> pd.DataFrame:
        """Add a weeknumber to the dataframe.

        Input: A dataframe where the index is the dates.

        Returns: A dataframe with a weeknum column.

        We want each week to start with Saturday and Sunday.
        """
        wk = 0
        wknum = []
        prev_val = 0
        for val in [int(str(x.isocalendar()[1])) for x in df.index]:
            if val != prev_val:
                wk += 1
            prev_val = val
            wknum.append(wk)
        df['weeknum'] = wknum
        indices = df.query("day in [5,6]").index
        df.loc[indices, 'weeknum'] = df.loc[indices, 'weeknum'] + 1
        return df

    df = create_wknum(df)
    sat_p = []
    sun_p = []
    for _, df2 in df.groupby(['weeknum']):
        if df2.shape[0] == 7:
            total_counts = df2[states][~df2.day.isin([5, 6])].sum()
            sat_p.append(df2[states][df2.day == 5].values[0] / total_counts)
            sun_p.append(df2[states][df2.day == 6].values[0] / total_counts)
    sat_vcorr = np.array([x if x > 0 else 0 for x in 0.2 - sum(sat_p) / len(sat_p)])
    sun_vcorr = np.array([x if x > 0 else 0 for x in 0.2 - sum(sun_p) / len(sun_p)])
    last_sat = 0
    last_sun = 0
    for _, df2 in df.groupby(['weeknum']):
        if df2.shape[0] == 7:
            total_counts = df2[states][~df2.day.isin([5, 6])].sum()
            df.loc[df2.index[df2.day == 5], states] += sat_vcorr * total_counts
            last_sat = sat_vcorr * total_counts
            df.loc[df2.index[df2.day == 6], states] += sun_vcorr * total_counts
            last_sun = sun_vcorr * total_counts
        else:
            df.loc[df2.index[df2.day == 5], states] = df.loc[df2.index[df2.day == 5],
                                                             states] + last_sat
            df.loc[df2.index[df2.day == 6], states] = df.loc[df2.index[df2.day == 6],
                                                             states] + last_sun
    return df.reset_index().rename(columns={'index': 'date'})


def ar_method(df: pd.DataFrame, states: list, num_lags: int, n_train: int,
              n_test: int, n_valid: int,  replace_df: pd.DataFrame) \
        -> (pd.DataFrame, pd.DataFrame):
    """Use an AR forecaster to predict the values of the current day.

    The parameters to the AR method is num_lags: the number of lags.

    For each of the n_test days, we create an AR model using n_train number
    of data points per state.
    Each model is tested for the n_test day and we save the residual.
    We create a residual distribution from #states * n_train number of residuals,
    which is used to rank the residuals from the days in n_valid.
    Inputs:
    df: Dataframe for the forecaster
    states: The names of the states
    num_lags: The number of lags for the AR model
    n_train: Number of data points to train each AR model
    n_test: Number of AR models per state to create residual distribution
    n_valid: Using the residual distribution to rank the points in n_valid.
    file_resid: Caching from any residual with compatible parameters
    Output:
    replace_df: A dataframe of residuals
    resid_valid: A dataframe of points and their rank as flags
    """
    dates_covered = []
    valid_day_list = list(df.date[-n_valid:])
    test_day_list = list(df.date[-n_test - n_valid:-n_valid])

    if not replace_df.empty:
        replace_df_orig = replace_df.reset_index().drop(columns=['lags','key'])
        replace_df_orig['date'] = pd.to_datetime(replace_df_orig['date'])
        dates_covered = np.unique(replace_df_orig.date)
    curr_day_list = df.date[-n_test - n_valid:][~df.date.isin(dates_covered)]
    replace_df = df.query('date in @curr_day_list')[states +
                ['date']].set_index('date').stack().reset_index()
    replace_df.columns = ['date', 'state', 'y']
    replace_df['y_pred'] = replace_df['y']


    lags_names = [f'lags_{i}' for i in range(1, num_lags + 1)]
    model_str = 'model ~ ' + '+'.join(lags_names)

    for curr_day in curr_day_list:
        start_train = curr_day - pd.Timedelta(days=n_train)
        all_tmp = df[start_train <= df.date]
        all_tmp = all_tmp[all_tmp.date <= curr_day][states]

        def pred_val(col):
            state_df = pd.DataFrame()
            state_df['model'] = col
            for i in range(1, num_lags + 1):
                state_df[f'lags_{i}'] = state_df['model'].shift(i)
            state_df = state_df.dropna()
            res_ols = smf.ols(model_str, state_df.iloc[:-2, :]).fit()
            y_t_pred = np.array(res_ols.predict(state_df.iloc[-2, :]))
            return y_t_pred[0]

        y_t_state = all_tmp.apply(pred_val, axis=0, result_type='reduce').T
        replace_df.loc[(replace_df.date == curr_day),
                       'y_pred'] = y_t_state.values

    replace_df['resid'] = round((replace_df['y_pred'] -
                                 replace_df['y']) / replace_df['y'], 6)

    resid_all = replace_df[replace_df.date.isin(test_day_list)]['resid'].values
    resid_valid = replace_df[replace_df.date.isin(valid_day_list)]

    resid_valid['cdf'] = resid_valid['resid'].apply(lambda x:
                                        (len(resid_all[resid_all < x]) / len(resid_all)))
    resid_valid['sort_prio'] = resid_valid['cdf'].apply(lambda x: x if x < 0.5 else 1 - x)
    resid_valid = resid_valid.reset_index(drop=True)
    resid_valid['state'] = resid_valid['state'].astype(int)

    return replace_df, resid_valid.drop_duplicates()
