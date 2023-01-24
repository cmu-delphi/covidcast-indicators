"""Functions pertaining to running FlaSH daily."""

import numpy as np
import pandas as pd
from scipy.stats import binom
import boto3
from delphi_utils.weekday import Weekday
from .constants import HTML_LINK, STATES, BUCKET
from .. import (
    get_structured_logger,
)


def split_reporting_schedule_dfs(input_df, flash_dir, lag):
    """Separate the input df by reporting schedule (pre-determined).

    Parameters
    ----------
    input_df: the df to split up
    flash_dir: the string reference directory to find all files
    lag: difference between reporting and reference date.

    Returns
    -------
    df of streams updated daily, less often than daily BUT has enough data to use the AR method
    and data that's updated very infrequently and there's not enough data to use the AR method.
    """
    rep_sched = pd.read_csv(f'{flash_dir}/reporting_sched_{lag}.csv', index_col=0)
    min_cut = rep_sched.loc['min_cut'][0]
    rep_sched = rep_sched.drop('min_cut')
    glob_out_list = []
    non_daily_ar = []
    for i, df in rep_sched.groupby('0'):
        fixed_sum = []
        columns = []
        for col in input_df.columns:
            if col in df.index:
                columns.append(col)
                fixed_sum.append(input_df[col])

        if len(fixed_sum) > 0:
            fixed_sum_df = pd.concat(fixed_sum).to_frame().T
            fixed_sum_df.columns = columns
            fixed_sum_df.index = [input_df.index[0]]
            if i ==1:
                daily_df = fixed_sum_df
            elif i >= min_cut:
                glob_out_list.append(fixed_sum_df)
            else:
                non_daily_ar.append(fixed_sum_df)
    return daily_df, pd.concat(non_daily_ar,axis=1) , pd.concat(glob_out_list, axis=1)


def bin_approach(y, yhat, pop, log=False):
    """Create test statistic.

    Parameters
    ----------
    y: observed values for streams
    yhat: predicted values for streams
    pop: population for a region
    log: difference between reporting and reference date

    Returns
    -------
    today's test-statistic values for the stream
    """
    def ts_dist(x, y, n):
        """Initialize test statistic distribution which is then vectorized."""
        # print(x, y,  y/n, n, binom.cdf(x, int(n), y/n)
        return binom.cdf(x, int(n), y/n)
    vec_ts_dist = np.vectorize(ts_dist)
    if log:
        return vec_ts_dist(np.log(y+2), np.log(yhat+2), np.log(pd.Series(pop)+2))
    return vec_ts_dist(y, yhat, pop)


def global_outlier_detect(df, mean, var):
    """Determine global outliers by using abs(t-statistic) > 5.

    Parameters
    ----------
    df: Current df to evaluate for global outliers
    mean: Mean needed for t-statistic calculation
    var: Variance for t-statistic calculation

    Returns
    -------
    The columns that are global outliers.
    """
    all_columns = list(df.columns)
    mean = mean[all_columns]
    var = var[all_columns]
    return df.columns[((abs(df.T.iloc[:, 0].sort_index() - mean.sort_index())/var.clip(1)).gt(5))]

def apply_ar(last_7, flash_dir, lag, weekday_correction, non_daily_df, fips_pop_table):
    """Predict y_hat using an AR model.

    Parameters
    ----------
    last_7: the prior 7 days
    flash_dir: the string reference directory to find all files
    lag: the difference between reporing and reference date
    weekday_correction: daily data after weekday correction has been applied
    non_daily_df: df of streams that are not updated daily
    fips_pop_table: df of fips to population

    Returns
    -------
    ts_streams: return of test statistic for the day's steams.
    """
    lin_coeff = pd.read_csv(f'{flash_dir}/lin_coeff_{lag}.csv', index_col=0)
    y = pd.concat([weekday_correction, non_daily_df], axis=1)
    y_hat = pd.Series([np.dot(lin_coeff[x], last_7[x]) for x in y.columns])
    ts_streams = pd.DataFrame(bin_approach(y, y_hat,
                            list(fips_pop_table[y.columns].iloc[0, :]), log=True),
                            columns=y.columns)
    return ts_streams

def output(evd_ranking, day, lag, signal, logger):
    """Write the top streams that warrant human inspection to the log.

    Parameters
    ----------
    evd_ranking: the ranking from EVD method (shown to users)
    day: reference date
    lag: difference between reference and report date
    signal: current signal
    logger: logger to write the output of FlaSH

    Returns
    -------
    None
    """
    starter_link = f"{HTML_LINK}{(day+pd.Timedelta(f'{lag}d')).strftime('%Y-%m_%d')}"
    p_text = ""
    for j, (index, value) in enumerate(evd_ranking.sort_values(ascending=False).iteritems()):
        if j < 30:
            start_link = f"{starter_link},{day.strftime('%Y-%m_%d')},{index}"
            p_text += f"\t{start_link}|*{index}*, {'{:.2f}'.format(value)}>\n"
    name = f"Signal: {signal} Lag: {lag}"
    logger.info(name, payload=p_text)


def evd_ranking_fn(ts_streams, flash_dir):
    """Create ranking using EVDs.

    Parameters
    ----------
    ts_streams: today's test-statistic values for the stream
    flash_dir: the string reference directory to find all files

    Returns
    -------
    evd_ranking: Ranking streams via the extreme value distribution
    """
    EVD_max = pd.read_csv(f'{flash_dir}/max.csv', index_col=0)
    EVD_min = pd.read_csv(f'{flash_dir}/min.csv', index_col=0)
    evd_ranking = pd.concat(
        [ts_streams.apply(lambda x: sum(x.values[0] <= EVD_min['0'])
                                  / EVD_min['0'].shape[0], axis=0).sort_values(),
         ts_streams.apply(lambda x: sum(x.values[0] >= EVD_max['0'])
                                  / EVD_max['0'].shape[0], axis=0).sort_values()],
        axis=1).max(axis=1)
    evd_ranking.name = 'evd_ranking'
    return evd_ranking


def streams_groups_fn(stream, ts_streams):
    """Create the ranking from streams using geographical groupings.

    Uses historical distribution from the test-statistics.

    Parameters
    ----------
    stream: historical test statistic csv
    ts_streams: today's test-statistic values for the stream

    Returns
    -------
    stream_group: the ranking using geographically group test statistic distributions
    """
    streams_groups = stream.copy()
    streams_state = stream[list(filter(lambda x: len(x) == 2,
                                       stream.columns))].unstack().dropna()  # .values
    streams_groups.columns = streams_groups.columns.str[:2]
    ranking_streams = {}
    for key, group in streams_groups.stack().reset_index().groupby('level_1'):
        for col, val in ts_streams.T.iterrows():
            if key == col[:2]:
                total_dist = pd.concat([group[0], streams_state]).reset_index(drop=True)
                ranking_streams[col] = sum(total_dist < val[0]) / total_dist.shape[0]
    stream_group = pd.Series(ranking_streams, name='stream_group')
    return stream_group


def setup_fips(flash_dir):
    """Set up fips related dictionaries and population table.

    Input: The directory location for files
    Output: conversion dictionary state to fips & population per fips df
    """
    fips_lookup = pd.read_csv(f'{flash_dir}/fips.csv',
                header=None).astype(str).set_axis(['FIPS', 'STATE'], axis=1)
    fips_lookup['FIPS'] = fips_lookup['FIPS'].str.zfill(2)
    fips_to_STATE = fips_lookup.set_index("FIPS").to_dict()['STATE']
    STATE_to_fips = fips_lookup.set_index("STATE").to_dict()['FIPS']
    fips = pd.read_csv(f'{flash_dir}/geo_fips.csv', index_col=0).set_index('geo')
    orig_fips = fips[fips['pop'].isna()].index
    geos_repl = [fips_to_STATE[x] for x in orig_fips.str[:2]]
    fips.loc[orig_fips, 'pop'] = [float(fips[fips.index == x]['pop']) for x in geos_repl]
    fips_pop_table = fips.unstack().to_frame().T
    fips_pop_table.columns = [STATE_to_fips[x] if x in list(STATES)
                          else x for x in fips_pop_table.columns.droplevel()]
    return STATE_to_fips, fips_pop_table
def flash_eval(lag, day, input_df, signal, params, logger=None):
    """Evaluate most recent data using FlaSH.

    Input:
    lag: the difference between the reporting and reference date
    day: the day of the reference date (today is the reporting date)
    input_df: a df from the day for a particular signal that includes natl. state, and county data
    params: additional params needed.
    Ouput:
    None
    """
    if not logger:
        logger = get_structured_logger(
            name=signal,
            filename=params["common"].get("log_filename", None),
            log_exceptions=params["common"].get("log_exceptions", True))

    #TODOv4: Change these to a local dir
    flash_dir = f'flash_ref/{signal}'
    last_7 = pd.read_csv(f'{flash_dir}/last_7_{lag}.csv', index_col=0).astype(float)
    wk_mean = pd.read_csv(f'{flash_dir}/weekday_mean_df_{lag}.csv', index_col=0)
    wk_var = pd.read_csv(f'{flash_dir}/weekday_var_df_{lag}.csv', index_col=0)
    weekday_params = pd.read_csv(f'{flash_dir}/weekday_params_{lag}.csv', index_col=0)
    summary_stats = pd.read_csv(f'{flash_dir}/summary_stats_{lag}.csv', index_col=0)
    stream = pd.read_csv(f'{flash_dir}/ret_df2_{lag}.csv', index_col=0)


    STATE_to_fips, fips_pop_table = setup_fips(flash_dir)
    input_df.columns = [str(STATE_to_fips[x]) if x in list(STATES)
                        else x for x in input_df.columns]

    # discuss where to do out-of-range handling
    out_range = input_df.columns[input_df.lt(params['flash']['support'][0]).iloc[0, :].values
                        & input_df.gt(params['flash']['support'][1]).iloc[0, :].values ]


    #only rank streams without out of range data
    input_df = input_df[filter(lambda x: x not in out_range, input_df.columns)]

    daily_update_df, non_daily_df_test, non_ar_df = split_reporting_schedule_dfs(input_df,
                                                                                 flash_dir, lag)
    # Weekday outlier [only for Daily Df]
    weekday_outlier = daily_update_df.columns[((abs(
        daily_update_df.T.sort_index()[day] - wk_mean.loc[day.day_of_week,
        daily_update_df.columns].sort_index()) /wk_var.loc[day.day_of_week,
        daily_update_df.columns].clip(1)).gt(5))]

    # Make weekday correction for daily update
    additive_factor = summary_stats[daily_update_df.columns].iloc[4, :]
    weekday_correction = (Weekday.calc_adjustment(
                          weekday_params.loc[daily_update_df.columns, :].to_numpy(), \
                          (daily_update_df + additive_factor).reset_index(),
                           daily_update_df.columns, \
                          'index').set_index('index') - additive_factor).clip(0)

    global_outlier_list = []
    for df in [weekday_correction, non_daily_df_test, non_ar_df]:
        global_outlier_list += list(
            global_outlier_detect(df, summary_stats[df.columns].iloc[2],
                                  summary_stats[df.columns].iloc[4]))

    # Apply AR
    ts_streams = apply_ar(last_7, flash_dir, lag, weekday_correction,
                        non_daily_df_test, fips_pop_table)
    # find stream ranking (individual)
    stream_individual = ts_streams.apply(
        lambda x: sum(x.values[0] <= stream[x.name].dropna()) /
                  stream[x.name].dropna().shape[0], axis=0)
    stream_individual.name = 'stream_individual'


    # find stream ranking (group)
    stream_group = streams_groups_fn(stream, ts_streams)

    # find EVD ranking
    evd_ranking = evd_ranking_fn(ts_streams, flash_dir)
    # Save the different categories of outliers/day + rankings for future analysis
    type_of_outlier = pd.DataFrame(index=input_df.columns)
    weekday_frame = weekday_outlier.to_frame()
    weekday_frame[0] = 1
    weekday_frame.columns = ['weekday_outlier']
    type_of_outlier = type_of_outlier.merge(weekday_frame,
            left_index=True, right_index=True, how='outer').fillna(0)
    type_of_outlier['global_outlier'] = 0
    type_of_outlier.loc[global_outlier_list, 'global_outlier'] = 1
    stream_group = stream_group.apply(lambda x: 2 * (0.5 - x) if x < 0.5 else x)
    stream_individual = stream_individual.apply(lambda x: 2 * (0.5 - x) if x < 0.5 else x)
    type_of_outlier = type_of_outlier.merge(stream_individual,
                      left_index=True, right_index=True,
                      how='outer').merge(stream_group,
                      left_index=True, right_index=True, how='outer').merge(evd_ranking,
                      left_index=True, right_index=True, how='outer'
                      )
    #if aws parameters are passed, save this dataframe to AWS
    if params.get('archive', None):
        if params['archive'].get("aws_credentials", None):
            session = boto3.Session(
                aws_access_key_id=params['archive']['aws_credentials']["aws_access_key_id"],
                aws_secret_access_key=params['archive']['aws_credentials']["aws_secret_access_key"])
            s3 = session.resource('s3')
            s3.Object(BUCKET,
                      f'flags-dev/flash_results/{signal}_{day.strftime("%m_%d_%Y")}_{lag}.csv').put(
                Body=type_of_outlier.to_csv(), ACL='public-read')

    not_fix_daily = list(filter(lambda x: x not in global_outlier_list, daily_update_df.columns))
    not_fix_last_7 = list(filter(lambda x: x not in not_fix_daily, last_7.columns))
    last_7 = pd.concat(
        [pd.concat([last_7[not_fix_daily].iloc[1:, :],
         weekday_correction[not_fix_daily]]).reset_index(drop=True),
         last_7[not_fix_last_7]], axis=1).to_csv(f'{flash_dir}/last_7_{lag}.csv')


    # Save to output log
    output(evd_ranking, day, lag, signal, logger)
