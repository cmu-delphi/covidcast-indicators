import datetime
import os
from typing import List
import numpy as np
import pandas as pd
import covidcast

from .constants import HOME_DWELL, COMPLETELY_HOME, FULL_TIME_WORK, PART_TIME_WORK
from .geo import FIPS_TO_STATE

# Magic number for modular arithmetic; CBG -> FIPS
MOD = 10000000


def date_from_fname(fname) -> datetime.date:
    _, year, month, day, __ = fname.rsplit('/', 4)
    return datetime.date(int(year), int(month), int(day))


def date_from_timestamp(timestamp) -> datetime.date:
    return datetime.date.fromisoformat(timestamp.split('T')[0])


def read_and_validate_file(fname) -> pd.DataFrame:
    df = pd.read_csv(fname)
    unique_date = df['timestamp'].unique()
    if len(unique_date) != 1:
        raise ValueError(
            f'More than one timestamp found in input file {fname}.')
    # assert date_from_timestamp(unique_date[0]) == date_from_fname(fname),\
    #     f'The name of input file {fname} does not correspond to the date'\
    #     'contained inside.'
    return df


def files_in_past_week(current_filename) -> List[str]:
    """Constructs file paths from previous 6 days.
    Parameters
    ----------
    current_filename: str
        name of CSV file.  Must be of the form
        {path}/{YYYY}/{MM}/{DD}/{YYYY}-{MM}-{DD}-social-distancing.csv.gz
    Returns
    -------
    List of file names corresponding to the 6 days prior to YYYY-MM-DD.
    """
    path, year, month, day, _ = current_filename.rsplit('/', 4)
    current_date = datetime.date(int(year), int(month), int(day))
    one_day = datetime.timedelta(days=1)
    for _ in range(1, 7):
        current_date = current_date - one_day
        y, m, d = (current_date.year, current_date.month, current_date.day)
        new_filename = f'{path}/{y}/{m}/{d}/{current_date.isoformat()}-'\
            'social-distancing.csv.gz'
        if os.path.exists(new_filename):
            yield new_filename


def add_prefix(signal_names, wip_signal, prefix: str):
    """Adds prefix to signal if there is a WIP signal
    Parameters
    ----------
    signal_names: List[str]
        Names of signals to be exported
    prefix : 'wip_'
        prefix for new/non public signals
    wip_signal : List[str] or bool
        a list of wip signals: [], OR
        all signals in the registry: True OR
        only signals that have never been published: False
    Returns
    -------
    List of signal names
        wip/non wip signals for further computation
    """

    if wip_signal is True:
        return [prefix + signal for signal in signal_names]
    if isinstance(wip_signal, list):
        make_wip = set(wip_signal)
        return [
            (prefix if signal in make_wip else "") + signal
            for signal in signal_names
        ]
    if wip_signal in {False, ""}:
        return [
            signal if public_signal(signal)
            else prefix + signal
            for signal in signal_names
        ]
    raise ValueError("Supply True | False or '' or [] | list()")


def public_signal(signal_):
    """Checks if the signal name is already public using COVIDcast
    Parameters
    ----------
    signal_ : str
        Name of the signal
    Returns
    -------
    bool
        True if the signal is present
        False if the signal is not present
    """
    epidata_df = covidcast.metadata()
    for index in range(len(epidata_df)):
        if epidata_df['signal'][index] == signal_:
            return True
    return False


def construct_signals(cbg_df, signal_names):
    """Construct Census-block level signals.
    In its current form, we prepare the following signals in addition to those
    already available in raw form from Safegraph:
    - completely_home_prop, defined as:
        completely_home_device_count / device_count
    - full_time_work_prop, defined as:
        full_time_work_behavior_devices / device_count
    - part_time_work_prop, defined as:
        part_time_work_behavior_devices / device_count
    Documentation for the social distancing metrics:
    https://docs.safegraph.com/docs/social-distancing-metrics
    Parameters
    ----------
    cbg_df: pd.DataFrame
        Census block group-level dataframe with raw social distancing
        indicators from Safegraph.
    signal_names: List[str]
        Names of signals to be exported.
    Returns
    -------
    pd.DataFrame
        Dataframe with columns: timestamp, county_fips, and
        {each signal described above}.
    """

    # Preparation
    cbg_df['county_fips'] = (cbg_df['origin_census_block_group'] // MOD).apply(
        lambda x: f'{int(x):05d}')

    # Transformation: create signal not available in raw data
    for signal in signal_names:
        if signal.endswith(FULL_TIME_WORK):
            cbg_df[signal] = (cbg_df['full_time_work_behavior_devices']
                              / cbg_df['device_count'])
        elif signal.endswith(COMPLETELY_HOME):
            cbg_df[signal] = (cbg_df['completely_home_device_count']
                              / cbg_df['device_count'])
        elif signal.endswith(PART_TIME_WORK):
            cbg_df[signal] = (cbg_df['part_time_work_behavior_devices']
                              / cbg_df['device_count'])
        elif signal.endswith(HOME_DWELL):
            cbg_df[signal] = (cbg_df['median_home_dwell_time'])

    # Subsetting
    return cbg_df[['county_fips'] + signal_names]


def aggregate(df, signal_names, geo_resolution='county'):
    '''Aggregate signals to appropriate resolution and produce standard errors.
    Parameters
    ----------
    df: pd.DataFrame
        County block group-level data with prepared signals (output of
        construct_signals().
    signal_names: List[str]
        Names of signals to be exported.
    geo_resolution: str
        One of ('county', 'state')
    Returns
    -------
    pd.DataFrame:
        DataFrame with one row per geo_id, with columns for the individual
        signals, standard errors, and sample sizes.
    '''
    # Prepare geo resolution
    GEO_RESOLUTION = ('county', 'state')
    if geo_resolution == 'county':
        df['geo_id'] = df['county_fips']
    elif geo_resolution == 'state':
        df['geo_id'] = df['county_fips'].apply(lambda x:
                                               FIPS_TO_STATE[x[:2]])
    else:
        raise ValueError(f'`geo_resolution` must be one of {GEO_RESOLUTION}.')

    # Aggregation and signal creation
    grouped_df = df.groupby(['geo_id'])[signal_names]
    df_mean = grouped_df.mean()
    df_sd = grouped_df.std()
    df_n = grouped_df.count()
    agg_df = pd.DataFrame.join(df_mean, df_sd,
                               lsuffix='_mean', rsuffix='_sd')
    agg_df = pd.DataFrame.join(agg_df, df_n.rename({
        signal: signal + '_n' for signal in signal_names
    }, axis=1))
    for signal in signal_names:
        agg_df[f'{signal}_se'] = (agg_df[f'{signal}_sd']
                                  / np.sqrt(agg_df[f'{signal}_n']))
    return agg_df.reset_index()


def process_single_date(df, signal_names, geo_resolutions, export_dir):
    """Process an input census block group-level CSV and export it.  Assumes
    that the input file has _only_ one date of data.
    Parameters
    ----------
    export_dir
        path where the output files are saved
    signal_names : List[str]
        signal names to be processed
    cbg_df: pd.DataFrame
        census block group-level CSV.
    geo_resolutions: List[str]
        List of geo resolutions to export the data.
    Returns
    -------
    None
    """
    date = date_from_timestamp(df.at[0, 'timestamp'])
    cbg_df = construct_signals(df, signal_names)
    for geo_res in geo_resolutions:
        aggregated_df = aggregate(cbg_df, signal_names, geo_res)
        for signal in signal_names:
            df_export = aggregated_df[
                ['geo_id']
                + [f'{signal}_{x}' for x in ('mean', 'se', 'n')]
            ].rename({
                f'{signal}_mean': 'val',
                f'{signal}_se': 'se',
                f'{signal}_n': 'sample_size',
            }, axis=1)
            df_export.to_csv(f'{export_dir}/{date}_{geo_res}_{signal}.csv',
                             na_rep='NA',
                             index=False, )


def process_windowed_average(df_list, signal_names, geo_resolutions, export_dir):
    date = date_from_timestamp(df_list[0].at[0, 'timestamp'])
    cbg_df = pd.concat(construct_signals(df, signal_names) for df in df_list)
    for geo_res in geo_resolutions:
        aggregated_df = aggregate(cbg_df, signal_names, geo_res)
        for signal in signal_names:
            df_export = aggregated_df[
                ['geo_id']
                + [f'{signal}_{x}' for x in ('mean', 'se', 'n')]
            ].rename({
                f'{signal}_mean': 'val',
                f'{signal}_se': 'se',
                f'{signal}_n': 'sample_size',
            }, axis=1)
            df_export.to_csv(f'{export_dir}/{date}_{geo_res}_{signal}.csv',
                             na_rep='NA',
                             index=False, )


def process(fname, signal_names, geo_resolutions, export_dir):
    past_week = [pd.read_csv(fname)]
    # past_week.extend(pd.read_csv(f)
    #                  for f in files_in_past_week(fname))

    process_single_date(past_week[0], signal_names,
                        geo_resolutions, export_dir)
    # process_windowed_average(past_week, signal_names,
    #                          geo_resolutions, export_dir)
