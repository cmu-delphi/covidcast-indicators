"""Internal functions for creating Safegraph indicator."""
import datetime
import os
from typing import List
import numpy as np
import pandas as pd
from delphi_utils.signal import add_prefix
from delphi_utils.export import create_export_csv
from delphi_utils.geomap import GeoMapper

from .constants import HOME_DWELL, COMPLETELY_HOME, FULL_TIME_WORK, PART_TIME_WORK, GEO_RESOLUTIONS

# Magic number for modular arithmetic; CBG -> FIPS
MOD = 10000000

# Base file name for raw data CSVs.
CSV_NAME = 'social-distancing.csv.gz'


def validate(df):
    """Confirm that a data frame has only one date."""
    timestamps = df['date_range_start'].apply(date_from_timestamp)
    assert len(timestamps.unique()) == 1


def date_from_timestamp(timestamp) -> datetime.date:
    """Extract the date from a timestamp beginning with {YYYY}-{MM}-{DD}T."""
    return datetime.date.fromisoformat(timestamp.split('T')[0])


def files_in_past_week(current_filename) -> List[str]:
    """Construct file paths from previous 6 days.

    Parameters
    ----------
    current_filename: str
        name of CSV file.  Must be of the form
        {path}/{YYYY}/{MM}/{DD}/{YYYY}-{MM}-{DD}-{CSV_NAME}
    Returns
    -------
    List of file names corresponding to the 6 days prior to YYYY-MM-DD.
    """
    path, year, month, day, _ = current_filename.rsplit('/', 4)
    current_date = datetime.date(int(year), int(month), int(day))
    one_day = datetime.timedelta(days=1)
    for _ in range(1, 7):
        current_date = current_date - one_day
        date_str = current_date.isoformat()
        date_path = date_str.replace('-', '/')
        new_filename = f'{path}/{date_path}/{date_str}-{CSV_NAME}'
        yield new_filename


def add_suffix(signals, suffix):
    """Add `suffix` to every element of `signals`."""
    return [s + suffix for s in signals]


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
        if FULL_TIME_WORK in signal:
            cbg_df[signal] = (cbg_df['full_time_work_behavior_devices']
                              / cbg_df['device_count'])
        elif COMPLETELY_HOME in signal:
            cbg_df[signal] = (cbg_df['completely_home_device_count']
                              / cbg_df['device_count'])
        elif PART_TIME_WORK in signal:
            cbg_df[signal] = (cbg_df['part_time_work_behavior_devices']
                              / cbg_df['device_count'])
        elif HOME_DWELL in signal:
            cbg_df[signal] = (cbg_df['median_home_dwell_time'])

    # Subsetting
    return cbg_df[['county_fips'] + signal_names]


def aggregate(df, signal_names, geo_resolution='county'):
    """Aggregate signals to appropriate resolution and produce standard errors.

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
    """
    # Prepare geo resolution
    gmpr = GeoMapper()
    if geo_resolution == 'county':
        geo_transformed_df = df.copy()
        geo_transformed_df['geo_id'] = df['county_fips']
    elif geo_resolution == 'state':
        geo_transformed_df = gmpr.add_geocode(df,
                                              from_col='county_fips',
                                              from_code='fips',
                                              new_code='state_id',
                                              new_col='geo_id',
                                              dropna=False)
    elif geo_resolution in ['msa', 'nation', 'hrr', 'hhs']:
        geo_transformed_df = gmpr.add_geocode(df,
                                              from_col='county_fips',
                                              from_code='fips',
                                              new_code=geo_resolution,
                                              new_col='geo_id',
                                              dropna=False)

    else:
        raise ValueError(
            f'`geo_resolution` must be one of {GEO_RESOLUTIONS}.')

    # Aggregation and signal creation
    grouped_df = geo_transformed_df.groupby(['geo_id'])[signal_names]
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


def process_window(df_list: List[pd.DataFrame],
                   signal_names: List[str],
                   geo_resolutions: List[str],
                   export_dir: str):
    """Process a list of input census block group-level data frames as a single data set and export.

    Assumes each data frame has _only_ one date of data.

    Parameters
    ----------
    cbg_df: pd.DataFrame
        list of census block group-level frames.
    signal_names: List[str]
        signal names to be processed
    geo_resolutions: List[str]
        List of geo resolutions to export the data.
    export_dir
        path where the output files are saved
    Returns
    -------
    None.  One file is written per (signal, resolution) pair containing the
    aggregated data from `df`.
    """
    for df in df_list:
        validate(df)
    date = date_from_timestamp(df_list[0].at[0, 'date_range_start'])
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
            df_export["timestamp"] = date.strftime('%Y%m%d')
            create_export_csv(df_export,
                              export_dir,
                              geo_res,
                              signal,
                              )


def process(filenames: List[str],
            signal_names: List[str],
            wip_signal,
            geo_resolutions: List[str],
            export_dir: str):
    """Create and exports signals corresponding both single day and averaged over the previous week.

    Parameters
    ----------
    current_filename: List[str]
        paths to files holding data.
        The first entry of the list should correspond to the target date while
        the remaining entries should correspond to the dates from each day in
        the week preceding the target date.
    signal_names: List[str]
        signal names to be processed for a single date.
        A second version of each such signal named {SIGNAL}_7d_avg will be
        created averaging {SIGNAL} over the past 7 days.
    wip_signal : List[str] or bool
        a list of wip signals: [], OR
        all signals in the registry: True OR
        only signals that have never been published: False
    geo_resolutions: List[str]
        List of geo resolutions to export the data.
    export_dir
        path where the output files are saved.
    Returns
    -------
    None.  For each (signal, resolution) pair, one file is written for the
    single date values to {export_dir}/{date}_{resolution}_{signal}.csv and
    one for the data averaged over the previous week to
    {export_dir}/{date}_{resolution}_{signal}_7d_avg.csv.
    """
    past_week = []
    for fname in filenames:
        if os.path.exists(fname):
            past_week.append(pd.read_csv(fname))

    # First process the current file alone...
    process_window(past_week[:1],
                   add_prefix(signal_names, wip_signal, 'wip_'),
                   geo_resolutions,
                   export_dir)
    # ...then as part of the whole window.
    process_window(past_week,
                   add_prefix(add_suffix(signal_names, '_7dav'),
                              wip_signal,
                              'wip_'),
                   geo_resolutions,
                   export_dir)
