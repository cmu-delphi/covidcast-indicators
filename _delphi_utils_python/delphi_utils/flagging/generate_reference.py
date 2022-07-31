"""Functions used to generate files related to the reference files."""
import numpy as np
import pandas as pd
import scipy.stats as stats
from ..weekday import Weekday


def identify_correct_spikes(df: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
    """We want to find large spikes in data and then correct them.

    We do this by considering the difference between weekdays ex: Sun-Mon
    and identifying points that have a z-score > 3 for each of the weekday diffs (6 categories).

    Then, we correct those points by using the median of
    the weekday diff for that particular category in sequential order.

    Input: A raw dataframe

    Return: A dataframe with spikes corrected, flagged points list
    """
    df['day'] = [x.weekday() for x in list(df.index)]
    df['end'] = [x.weekday() in [5, 6] for x in list(df.index)]
    diff_df = df.drop(columns=['end', 'day']).diff(1).dropna()
    diff_df['day'] = df['day']
    medians = []
    for _, (_, ldf) in enumerate(diff_df.groupby(['day'])):
        medians.append(ldf.apply(lambda x: np.median(pd.Series(x).values), axis=0))
    diff_df = diff_df.drop(columns=['day'])
    points_out = diff_df.apply(lambda x: []+ list(x[stats.zscore(x) > 3].index)
                                         + list(x[stats.zscore(x) < -3].index), axis=0)
    medians_df = pd.concat(medians, axis=1).T
    point_out_df = []
    for state, points_list in zip(list(points_out.index), points_out):
        point_out_df.append(df.loc[points_list, state])

        for po in points_list:
            day_week = df.loc[po, 'day']
            po2 = po - pd.Timedelta('1d')
            df.loc[po, state] = df.loc[po2, state] + medians_df.loc[day_week, state]
    if pd.DataFrame(point_out_df).empty:
        return df, pd.DataFrame()
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
    if (len(sat_p) > 1) and (len(sun_p) > 1):
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


def gen_ref_dfs(df: pd.DataFrame, logger) -> pd.DataFrame:
    """ Method to generate all the reference files given the raw datafile.
     Outputs a dictionary of reference files.
    """

    #Extrapolate between missing dates
    df = df.fillna(method='ffill', axis=0).fillna(method='bfill', axis=0)
    df = df.sort_index().reset_index().drop_duplicates().set_index("index")
    assert True not in df.index.duplicated(), \
        'Unable to proceed, multiple dates with conflicting values'
    if not df.empty:
        dates_range = pd.date_range(df.index.min(), df.index.max())
        merge_files = pd.DataFrame(index=dates_range)
        df = merge_files.merge(df, how='outer', left_index=True,
                                      right_index=True).fillna(method='ffill')
        df.index = pd.to_datetime(df.index.date)

        #this restriction is due to Weekday.py functionality
    assert df.shape[0] >= 7, "Need at least seven dates for Weekday.py"

    spikes_df, flags = identify_correct_spikes(df.copy())
    if not flags.empty:
        flags = flags.reset_index()
        flags.columns = ['state', 'date', 'val']
    weekend_df = weekend_corr(spikes_df.copy(), df.columns)
    params = Weekday.get_params(weekend_df.copy(), None, df.columns, 'date',
                                [1, 1e5, 1e10, 1e15], logger, 10)
    weekday_corr = Weekday.calc_adjustment(params
                    ,weekend_df.copy(), df.columns,
                    'date').fillna(0).drop(columns=['day', 'end','weeknum'])
    return {'ref_dfs/raw.csv': df,
            'ref_dfs/spikes.csv': spikes_df,
            'ref_dfs/wknd.csv': weekend_df.set_index('date'),
            'ref_dfs/wkdy_corr.csv': weekday_corr.set_index('date'),
            'ref_dfs/flag_spike.csv': flags}
