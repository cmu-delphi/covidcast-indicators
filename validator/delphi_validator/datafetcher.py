# -*- coding: utf-8 -*-
"""
Functions to get CSV filenames and data.
"""

import re
from os import listdir
from os.path import isfile, join
from datetime import datetime
from itertools import product
import pandas as pd
import numpy as np

import covidcast
from .errors import APIDataFetchError

filename_regex = re.compile(
    r'^(?P<date>\d{8})_(?P<geo_type>\w+?)_(?P<signal>\w+)\.csv$')


def read_filenames(path):
    """
    Return a list of tuples of every filename and regex match to the CSV filename
     format in the specified directory.

    Arguments:
        - path: path to the directory containing CSV data files.

    Returns:
        - list of tuples
    """
    daily_filenames = [(f, filename_regex.match(f))
                       for f in listdir(path) if isfile(join(path, f))]
    return daily_filenames


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


def get_geo_signal_combos(data_source):
    """
    Get list of geo type-signal type combinations that we expect to see, based on
    combinations reported available by COVIDcast metadata.
    """
    meta = covidcast.metadata()
    source_meta = meta[meta['data_source'] == data_source]
    unique_signals = source_meta['signal'].unique().tolist()
    unique_geotypes = source_meta['geo_type'].unique().tolist()

    geo_signal_combos = list(product(unique_geotypes, unique_signals))
    print("Number of expected geo region-signal combinations:",
          len(geo_signal_combos))

    return geo_signal_combos


def fetch_api_reference(data_source, start_date, end_date, geo_type, signal_type):
    """
    Get and process API data for use as a reference. Formatting is changed
    to match that of source data CSVs.
    """
    api_df = covidcast.signal(
        data_source, signal_type, start_date, end_date, geo_type)

    if not isinstance(api_df, pd.DataFrame):
        custom_msg = "Error fetching data from " + str(start_date) + \
                     " to " + str(end_date) + \
                     "for data source: " + data_source + \
                     ", signal type: " + signal_type + \
                     ", geo type: " + geo_type

        raise APIDataFetchError(custom_msg)

    column_names = ["geo_id", "val",
                    "se", "sample_size", "time_value"]

    # Replace None with NA to make numerical manipulation easier.
    # Rename and reorder columns to match those in df_to_test.
    api_df = api_df.replace(
        to_replace=[None], value=np.nan
    ).rename(
        columns={'geo_value': "geo_id", 'stderr': 'se', 'value': 'val'}
    ).drop(
        ['direction', 'issue', 'lag'], axis=1
    ).reindex(columns=column_names)

    return api_df
