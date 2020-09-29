# -*- coding: utf-8 -*-
"""
Functions to get CSV filenames and data.
"""

import re
from os import listdir
from os.path import isfile, join
from datetime import datetime
from typing import List
from itertools import product
import pandas as pd

import covidcast
from .errors import APIDataFetchError

filename_regex = re.compile(r'^(?P<date>\d{8})_(?P<geo_type>\w+?)_(?P<signal>\w+)\.csv$')


def get_filenames_with_geo_signal(path, data_source, date_slist: List[str]):
    """
    Gets list of filenames in data folder and list of expected geo type-signal type combinations.

    Arguments:
        - path: path to data CSVs
        - data_source: str; data source name, one of
        https://cmu-delphi.github.io/delphi-epidata/api/covidcast_signals.html
        - date_slist: list of dates (formatted as strings) to check

    Returns:
        - list of filenames
        - list of geo type-signal type combinations that we expect to see
    """
    geo_sig_cmbo = get_geo_sig_cmbo(data_source)

    for cmb in geo_sig_cmbo:
        print(cmb)

    filenames = read_relevant_date_filenames(path, date_slist[0])
    return filenames, geo_sig_cmbo


def get_geo_sig_cmbo(data_source):
    """
    Get list of geo type-signal type combinations that we expect to see, based on
    combinations reported available by Covidcast metadata.
    """
    meta = covidcast.metadata()
    source_meta = meta[meta['data_source']==data_source]
    unique_signals = source_meta['signal'].unique().tolist()
    unique_geotypes = source_meta['geo_type'].unique().tolist()

    if data_source == 'fb-survey':
        # Currently metadata returns --*community*-- signals that don't get generated
        # in the new fb-pipeline. Seiving them out for now.
        # TODO: Include weighted whh_cmnty_cli and wnohh_cmnty_cli
        for sig in unique_signals:
            if "community" in sig:
                unique_signals.remove(sig)

    geo_sig_cmbo = list(product(unique_geotypes, unique_signals))
    print("Number of mixed types:", len(geo_sig_cmbo))

    return geo_sig_cmbo


def read_filenames(path):
    """
    Return a list of tuples of every filename and regex match to the CSV filename format in the specified directory.

    Arguments:
        - path: path to the directory containing CSV data files.

    Returns:
        - list of tuples
    """
    daily_filenames = [ (f, filename_regex.match(f)) for f in listdir(path) if isfile(join(path, f))]
    return daily_filenames

def read_relevant_date_filenames(data_path, date_slist):
    """
    Return a list of tuples of every filename in the specified directory if the file is in the specified date range.

    Arguments:
        - data_path: path to the directory containing CSV data files.
        - date_slist: list of dates (formatted as strings) to check

    Returns:
        - list
    """
    all_files = [f for f in listdir(data_path) if isfile(join(data_path, f))]
    filenames = list()

    for fl in all_files:
        for dt in date_slist:
            if fl.find(dt) != -1:
                filenames.append(fl)
    return filenames

def read_geo_sig_cmbo_files(geo_sig_cmbo, data_folder, filenames, date_slist):
    """
    Generator that assembles data within the specified date range for a given geo_sig_cmbo.

    Arguments:
        - geo_sig_cmbo: list of geo type-signal type combinations that we expect to see, based on combinations reported available by Covidcast metadata
        - data_folder: path to the directory containing CSV data files.
        - filenames: list of filenames
        - date_slist: list of dates (formatted as strings) to check

    Returns:
        - dataframe containing data for all dates in date_slist for a given geo type-signal type combination
        - relevant geo type (str)
        - relevant signal type (str)
    """
    for geo_sig in geo_sig_cmbo:
        df_list = list()

        # Get all filenames for this geo_type and signal_type
        files = [file for file in filenames if geo_sig[0] in file and geo_sig[1] in file]

        if len(files) == 0:
            print("FILE_NOT_FOUND: File with geo_type:", geo_sig[0], " and signal:", geo_sig[1], " does not exist!")
            yield pd.DataFrame(), geo_sig[0], geo_sig[1]
            continue

        # Load data from all found files.
        for f in files:
            df = load_csv(join(data_folder, f))
            for dt in date_slist:

                # Add data's date, from CSV name, as new column
                if f.find(dt) != -1:
                    gen_dt = datetime.strptime(dt, '%Y%m%d')
                    df['time_value'] = gen_dt
            df_list.append(df)

        yield pd.concat(df_list), geo_sig[0], geo_sig[1]

def load_csv(path):
    """
    Load CSV with specified column types.
    """
    return pd.read_csv(
        path,
        dtype={
            'geo_id': str,
            'val': float,
            'se': float,
            'sample_size': float,
        })

def fetch_daily_data(data_source, survey_date, geo_type, signal):
    """
    Get API data for a specified date, source, signal, and geo type.
    """
    data_to_reference = covidcast.signal(data_source, signal, survey_date, survey_date, geo_type)
    if not isinstance(data_to_reference, pd.DataFrame):
        custom_msg = "Error fetching data on" + str(survey_date)+ \
                     "for data source:" + data_source + \
                     ", signal-type:"+ signal + \
                     ", geography-type:" + geo_type
        raise APIDataFetchError(custom_msg)
    return data_to_reference
