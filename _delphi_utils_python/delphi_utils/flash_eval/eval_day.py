"""Functions pertaining to running FlaSH daily."""
import io
import zipfile
import numpy as np
import pandas as pd
from scipy.stats import binom
import boto3
from delphi_utils.geomap import GeoMapper
from delphi_utils.weekday import Weekday
from .constants import HTML_LINK, STATES
from .. import (
    get_structured_logger,
)



def split_reporting_schedule_dfs(input_df, rep_sched):
    """Separate the input df by reporting schedule (pre-determined).

    Parameters
    ----------
    input_df: the df to split up
    rep_sched: a series of regions and their reporting schedules

    Returns
    -------
    df of streams updated daily, less often than daily BUT has enough data to use the AR method
    and data that's updated very infrequently and there's not enough data to use the AR method.
    """
    min_cut = rep_sched.loc['min_cut'][0]
    rep_sched = rep_sched.drop('min_cut')
    glob_out_list = []
    non_daily_ar = []
    rep_sched.columns = ['schedule']
    for i, df in rep_sched.groupby('schedule'):
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
    return (daily_df, pd.concat(non_daily_ar,axis=1) , pd.concat(glob_out_list, axis=1))


def bin_approach(df, log=False):
    """Create test statistic.

    Parameters
    ----------
    df with columns of
    y: observed values for streams
    yhat: predicted values for streams
    pop: population for a region

    log: taking the log for the test statistic measure

    Returns
    -------
    today's test-statistic values for the stream
    """
    def ts_dist(x, y, n):
        """Initialize test statistic distribution which is then vectorized."""
        return binom.cdf(x, int(n), y / n)

    vec_ts_dist = np.vectorize(ts_dist)
    if log:
        return pd.DataFrame(vec_ts_dist(np.log(df.y + 2),
                        np.log(df.yhat + 2), np.log(df['pop'] + 2)),
                            index=df.index)
    return pd.DataFrame(vec_ts_dist(df.y, df.yhat, df.pop), index=df.index)




def outlier_detect(df):
    """Determine global outliers by using abs(t-statistic) > 5.

    Parameters
    ----------
    df: Current df to evaluate for global outliers with columns
    for mean and var.

    Returns
    -------
    The zscore with respect to the empirical distribution
    """
    df.columns = ['x', 'mean', 'var']
    ret_series = abs(df['x'] - df['mean']) / (df['var'].clip(1))
    ret_series.index = df.index
    return ret_series

def apply_ar(last_7, lin_coeff, weekday_correction, non_daily_df, fips_pop_table):
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
    df_for_ts: dataframe for the test-statistic
    """
    y = pd.concat([weekday_correction, non_daily_df], axis=1)
    y.index = ['y']
    y_hat = pd.Series([np.dot(lin_coeff[x], last_7[x]) for x in last_7.columns], name='yhat')
    y_hat.index = last_7.columns
    df_for_ts = y.T.merge(y_hat, left_index=True,
                          right_index=True).merge(fips_pop_table,
                          left_index=True, right_index=True)
    df_for_ts.columns = ['y', 'yhat', 'pop']
    ts_streams = bin_approach(df_for_ts, log=True)
    ts_streams.columns = ['test-statistic']
    return ts_streams, df_for_ts


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
    for j, (index, value) in enumerate(evd_ranking.sort_values(ascending=False).items()):
        if j < 30:
            start_link = f"{starter_link},{day.strftime('%Y-%m_%d')},{index}"
            p_text += f"\t{start_link}|*{index}*, {'{:.2f}'.format(value)}>\n"
        else:
            break
    name = f"Signal: {signal} Lag: {lag}"
    logger.info(name, payload=p_text)


def evd_ranking_fn(ts_streams, EVD_max, EVD_min):
    """Create ranking using EVDs.

    Parameters
    ----------
    ts_streams: today's test-statistic values for the stream
    EVD_max: The maximum distribution for comparison
    EVD_min: The minimum distribution for comparison

    Returns
    -------
    evd_ranking: Ranking streams via the extreme value distribution
    """
    evd_ranking = pd.concat([ts_streams.apply(lambda x: ts_val(x.values[0],
                             EVD_min['0']), axis=1).sort_values(),
                              ts_streams.apply(lambda x:
                              1 - ts_val(x.values[0], EVD_max['0']),
                              axis=1).sort_values()], axis=1).max(axis=1)
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
                ranking_streams[col] = ts_val(val[0], total_dist)
    stream_group = pd.Series(ranking_streams, name='stream_group')
    return stream_group


def setup_fips():
    """Set up fips related dictionaries and population table.

    Output: conversion dictionary state to fips & population per fips df
    """
    gmpr = GeoMapper()
    STATE_to_fips = gmpr.get_crosswalk("state", "state")
    STATE_to_fips = pd.DataFrame(STATE_to_fips)[["state_id",
        "state_code"]].set_index("state_id").to_dict()["state_code"]
    state_df = gmpr.get_crosswalk("state_code", "pop")
    state_df.columns = ['geo', 'pop']
    fips_df = gmpr.get_crosswalk("fips", "pop")
    fips_df.columns = ['geo', 'pop']
    natl_df = gmpr.get_crosswalk("nation", "pop")
    natl_df.columns = ['geo', 'pop']
    fips_pop_table = pd.concat([state_df, fips_df, natl_df])
    fips_pop_table = fips_pop_table.set_index('geo')


    return STATE_to_fips, fips_pop_table


def ts_val(val, dist):
    """Determine p-value from the test statistic distribution.

    Parameters
    ----------
    val: The test statistic
    dist: The distribution to compare to

    Returns: p-value
    -------

    """
    return sum(val <= dist) / dist.shape[0]


def generate_files(params, lag, signal, local=False, s3=None):
    """Generate files needed for evaluation.

    Parameters
    ----------
    params: params from json file
    lag, signal: as specified on filesystem for e.g. params.zip (signal) and inner weekday csv (lag)
    local, s3: determine if files are local or on AWS

    Returns: all files needed for evaluation
    """
    if not local:
        with io.BytesIO(s3.Object(params['flash']["aws_bucket"],
                f'flags-dev/flash_params/{signal}/params.zip').get()['Body'].read()) as readio:
            with zipfile.ZipFile(readio, mode='r') as zipf:
                wk_mean = pd.read_csv(zipf.open(f'params/weekday_mean_df_{lag}.csv'), index_col=0)
                wk_var = pd.read_csv(zipf.open(f'params/weekday_var_df_{lag}.csv'), index_col=0)
                weekday_params = pd.read_csv(zipf.open(f'params/weekday_params_{lag}.csv'),
                                             index_col=0)
                summary_stats = pd.read_csv(zipf.open(f'params/summary_stats_{lag}.csv'),
                                            index_col=0)
                summary_stats.index = ['0.25', 'median', '0.75', 'mean', 'var']
                stream = pd.read_csv(zipf.open(f'params/ret_df2_{lag}.csv'), index_col=0)
                rep_sched = pd.read_csv(zipf.open(f'params/reporting_sched_{lag}.csv'), index_col=0)
                lin_coeff = pd.read_csv(zipf.open(f'params/lin_coeff_{lag}.csv'), index_col=0)
                EVD_max = pd.read_csv(zipf.open('params/max.csv'), index_col=0)
                EVD_min = pd.read_csv(zipf.open('params/min.csv'), index_col=0)
        last_7 = pd.read_csv(s3.Object(params['flash']["aws_bucket"],
                        f'flags-dev/flash_params/{signal}/last_7_{lag}.csv').get()['Body'],
                             index_col=0)
        STATE_to_fips, fips_pop_table = setup_fips()
    else:
        wk_mean = pd.read_csv((f'flash_ref/{signal}/weekday_mean_df_{lag}.csv'), index_col=0)
        wk_var = pd.read_csv((f'flash_ref/{signal}/weekday_var_df_{lag}.csv'), index_col=0)
        weekday_params = pd.read_csv((f'flash_ref/{signal}/weekday_params_{lag}.csv'), index_col=0)
        summary_stats = pd.read_csv((f'flash_ref/{signal}/summary_stats_{lag}.csv'), index_col=0)
        summary_stats.index = ['0.25', 'median', '0.75', 'mean', 'var']
        stream = pd.read_csv((f'flash_ref/{signal}/ret_df2_{lag}.csv'), index_col=0)
        rep_sched = pd.read_csv((f'flash_ref/{signal}/reporting_sched_{lag}.csv'), index_col=0)
        lin_coeff = pd.read_csv((f'flash_ref/{signal}/lin_coeff_{lag}.csv'), index_col=0)
        EVD_max = pd.read_csv((f'flash_ref/{signal}/max.csv'), index_col=0)
        EVD_min = pd.read_csv((f'flash_ref/{signal}/min.csv'), index_col=0)
        last_7 = pd.read_csv((f'flash_ref/{signal}/last_7_{lag}.csv'), index_col=0)
        STATE_to_fips, fips_pop_table = setup_fips()
    return wk_mean, wk_var, weekday_params, summary_stats, stream, rep_sched, \
                lin_coeff, EVD_max, EVD_min, last_7, STATE_to_fips, fips_pop_table

def process_params(lag, day, input_df, signal, params, logger, local=False):
    """Evaluate most recent data using FlaSH.

    Input:
    lag: the difference between the reporting and reference date
    day: the day of the reference date (today is the reporting date)
    input_df: a df from the day for a particular signal that includes natl. state, and county data
    signal: the signal to search for.
    params: additional params needed.
    logger: external logger to save error messages and some FlaSH Output (to send to Slack).
    local: controls if the dependent files for FlaSH will be available in a 'flash_ref'
    directory or should be pulled from the AWS bucket (more frequently updated).
    Ouput:
    last_7: A dataframe with the final 7 days
    type_of_outlier:  dataframe with many types of outliers
    """
    s3=None
    if not local:
        s3 = boto3.Session(
            aws_access_key_id=params['archive']['aws_credentials']["aws_access_key_id"],
            aws_secret_access_key=params['archive']['aws_credentials']["aws_secret_access_key"]
        ).resource('s3')

    (wk_mean, wk_var, weekday_params,
    summary_stats, stream, rep_sched, \
    lin_coeff, EVD_max, EVD_min, last_7, \
    STATE_to_fips, fips_pop_table) = generate_files(params, lag, signal, local=local, s3=s3)

    input_df.columns = [str(STATE_to_fips[x]) if x in list(STATES)
                        else x for x in input_df.columns]
    # discuss where to do out-of-range handling
    out_range = input_df.columns[input_df.lt(int(params['flash']['support'][0])).iloc[0, :].values
                        | input_df.gt(int(params['flash']['support'][1])).iloc[0, :].values]

    # only rank streams without out of range data
    input_df = input_df[filter(lambda x: x not in out_range, input_df.columns)]
    daily_update_df, non_daily_df_test, non_ar_df = split_reporting_schedule_dfs(input_df,
                                                                                 rep_sched)

    # only consider non-daily values that are non-0
    non_daily_df_test = non_daily_df_test[non_daily_df_test != 0].dropna(axis=1)

    # Weekday outlier [only for Daily Df]
    weekday_outlier = outlier_detect(daily_update_df.T.merge(wk_mean.loc[day.day_of_week, :],
        left_index=True, right_index=True).merge(
        wk_var.loc[day.day_of_week, :],
        left_index=True, right_index=True))

    # Make weekday correction for daily update
    additive_factor = 1
    weekday_correction = (Weekday.calc_adjustment(
        weekday_params.loc[daily_update_df.columns, :].to_numpy(), \
        (daily_update_df + additive_factor).reset_index(),
        daily_update_df.columns, \
        'index').set_index('index') - additive_factor).clip(0)

    global_outlier_list = []
    for df in [weekday_correction, non_daily_df_test, non_ar_df]:
        global_outlier_list.append(outlier_detect(df.T.merge(summary_stats[df.columns].loc[
                  'median', :], left_index=True, right_index=True).merge(
                   summary_stats[df.columns].loc['var', :],
                   left_index=True, right_index=True)))

    # Apply AR

    ts_streams, df_for_ts = apply_ar(last_7, lin_coeff, weekday_correction,
                                     non_daily_df_test, fips_pop_table)
    # find stream ranking (individual)
    stream_individual = ts_streams.T.apply(lambda x: ts_val(x.values[0],
                                                            stream[x.name].dropna()))

    stream_individual.name = 'stream_individual'

    # find stream ranking (group)
    stream_group = streams_groups_fn(stream, ts_streams)

    # find EVD ranking

    evd_ranking = evd_ranking_fn(ts_streams, EVD_max, EVD_min)
    # Save the different categories of outliers/day + rankings for future analysis
    type_of_outlier = pd.DataFrame(index=input_df.columns)
    type_of_outlier = type_of_outlier.merge(weekday_outlier.to_frame(name='weekday'),
                        left_index=True, right_index=True,
                        how='outer').fillna(0)
    glob = pd.concat(global_outlier_list)
    glob.name = 'global'
    type_of_outlier = type_of_outlier.merge(glob,
                        left_index=True, right_index=True, how='outer').fillna(0)

    stream_group = stream_group.apply(lambda x: 2 * (0.5 - x) if x < 0.5 else 2 * (x - 0.5))
    stream_individual = stream_individual.apply(lambda x: 2 * (0.5 - x) if
    x < 0.5 else 2 * (x - 0.5))

    type_of_outlier = type_of_outlier.merge(stream_individual,
        left_index=True, right_index=True,
        how='outer').merge(stream_group,
        left_index=True, right_index=True,
        how='outer').merge(evd_ranking,
        left_index=True,
        right_index=True, how='outer'
        ).merge(df_for_ts,
        left_index=True,
        right_index=True,
        how='outer').merge(
        ts_streams,
        left_index=True, right_index=True, how='outer')
    type_of_outlier['flash'] = type_of_outlier['evd_ranking']
    indices = type_of_outlier.index[type_of_outlier['evd_ranking'].isna()]
    type_of_outlier.loc[indices, 'flash'] = type_of_outlier.loc[indices, 'global']
    not_fix_daily = list(filter(lambda x: x not in pd.concat(global_outlier_list).index,
                                daily_update_df.columns))
    not_fix_last_7 = list(filter(lambda x: x not in not_fix_daily, last_7.columns))
    last_7 = pd.concat(
        [pd.concat([last_7[not_fix_daily].iloc[1:, :],
                    weekday_correction[not_fix_daily]]).reset_index(drop=True),
         last_7[not_fix_last_7]], axis=1)
    if not local:
        s3.Object(params['flash']["aws_bucket"],
                  f'flags-dev/flash_results/{signal}_{day.strftime("%m_%d_%Y")}_{lag}.csv').put(
            Body=type_of_outlier.to_csv(), ACL='public-read')
        s3.Object(params['flash']["aws_bucket"],
                  f'flags-dev/flash_params/{signal}/last_7_{lag}.csv').put(
            Body=last_7.to_csv(), ACL='public-read')
    # Save to output log
    output(evd_ranking, day, lag, signal, logger)
    return last_7, type_of_outlier


def flash_eval(lag, day, input_df, signal, params, logger=None, local=False):
    """Call fn to evaluate most recent data using FlaSH.

    Input:
    lag: the difference between the reporting and reference date
    day: the day of the reference date (today is the reporting date)
    input_df: a df from the day for a particular signal that includes natl. state, and county data
    signal: the name of the signal
    params: additional params needed.
    logger: external logger to save error messages and some FlaSH Output (to send to Slack).
    local: controls if the dependent files for FlaSH will be available in a 'flash_ref'
    Ouput:
    Returns past 7 days and the all outliers dataframe
    """
    if not logger:
        logger = get_structured_logger(
            name=signal,
            filename=params["common"].get("log_filename", None),
            log_exceptions=params["common"].get("log_exceptions", True))
    return process_params(lag, day, input_df, signal, params, logger, local)
