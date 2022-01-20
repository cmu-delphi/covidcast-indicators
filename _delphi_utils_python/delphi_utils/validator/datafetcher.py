# -*- coding: utf-8 -*-
"""Functions to get CSV filenames and data."""

import re
import threading
from os import listdir
from os.path import isfile, join
import warnings
import requests
import pandas as pd
import numpy as np

import covidcast
from .errors import APIDataFetchError, ValidationFailure

FILENAME_REGEX = re.compile(
    r'^(?P<date>\d{8})_(?P<geo_type>\w+?)_(?P<signal>\w+)\.csv$')


def make_date_filter(start_date, end_date):
    """
    Create a function to filter dates in the specified date range (inclusive).

    Arguments:
        - start_date: datetime date object
        - end_date: datetime date object

    Returns:
        - Custom function object
    """
    # Convert dates from datetime format to int.
    start_code = int(start_date.strftime("%Y%m%d"))
    end_code = int(end_date.strftime("%Y%m%d"))

    def custom_date_filter(match):
        """
        Determine if a single filename is in the date range.

        Arguments:
            - match: regex match object based on FILENAME_REGEX applied to a filename str

        Returns:
            - boolean
        """
        # If regex match doesn't exist, current filename is not an appropriately
        # formatted source data file.
        if not match:
            return False

        # Convert date found in CSV name to int.
        code = int(match.groupdict()['date'])

        # Return boolean True if current file date "code" is within the defined date range.
        return start_code <= code <= end_code

    return custom_date_filter


def load_all_files(export_dir, start_date, end_date):
    """Load all files in a directory.

    Parameters
    ----------
    export_dir: str
        directory from which to load files

    Returns
    -------
    loaded_data: List[Tuple(str, re.match, pd.DataFrame)]
        triples of filenames, filename matches with the geo regex, and the data from the file
    """
    export_files = read_filenames(export_dir)
    date_filter = make_date_filter(start_date, end_date)

    # Make list of tuples of CSV names and regex match objects.
    return [(f, m, load_csv(join(export_dir, f))) for (f, m) in export_files if date_filter(m)]


def read_filenames(path):
    """
    Read all file names from `path` and match them against FILENAME_REGEX.

    Arguments:
        - path: path to the directory containing CSV data files.

    Returns:
        - list of tuples of every filename and regex match to the CSV filename
          format in the specified directory
    """
    daily_filenames = [(f, FILENAME_REGEX.match(f))
                       for f in listdir(path) if isfile(join(path, f))]
    return daily_filenames


def load_csv(path):
    """Load CSV with specified column types."""
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
    Get list of geo type-signal type combinations that we expect to see.

    Cross references based on combinations reported available by COVIDcast metadata.
    """
    # Maps data_source name with what's in the API, lists used in case of multiple names

    source_signal_mappings = {i['source']:i['db_source'] for i in
        requests.get("https://api.covidcast.cmu.edu/epidata/covidcast/meta").json()}
    meta = covidcast.metadata()
    source_meta = meta[meta['data_source'] == data_source]
    # Need to convert np.records to tuples so they are hashable and can be used in sets and dicts.
    geo_signal_combos = list(map(tuple,
                                 source_meta[["geo_type", "signal"]].to_records(index=False)))
    # Only add new geo_sig combos if status is active
    new_geo_signal_combos = []
    # Use a seen dict to save on multiple calls:
    # True/False indicate if status is active, "unknown" means we should check
    sig_combo_seen = dict()
    for combo in geo_signal_combos:
        if data_source in source_signal_mappings.values():
            src_list = [key for (key, value) in source_signal_mappings.items()
                if value == data_source]
        else:
            src_list = [data_source]
        for src in src_list:
            sig = combo[1]
            geo_status = sig_combo_seen.get((sig, src), "unknown")
            if geo_status is True:
                new_geo_signal_combos.append(combo)
            elif geo_status == "unknown":
                epidata_signal = requests.get(
                    "https://api.covidcast.cmu.edu/epidata/covidcast/meta",
                    params={'signal': f"{src}:{sig}"})
                # Not an active signal
                active_status = [val['active'] for i in epidata_signal.json()
                    for val in i['signals']]
                if active_status == []:
                    sig_combo_seen[(sig, src)] = False
                    continue
                sig_combo_seen[(sig, src)] = active_status[0]
                if active_status[0] is True:
                    new_geo_signal_combos.append(combo)
    return new_geo_signal_combos

def fetch_api_reference(data_source, start_date, end_date, geo_type, signal_type):
    """
    Get and process API data for use as a reference.

    Formatting is changed to match that of source data CSVs.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        api_df = covidcast.signal(
            data_source, signal_type, start_date, end_date, geo_type)

    if not isinstance(api_df, pd.DataFrame):
        custom_msg = "Error fetching data from " + str(start_date) + \
                     " to " + str(end_date) + \
                     " for data source: " + data_source + \
                     ", signal type: " + signal_type + \
                     ", geo type: " + geo_type

        raise APIDataFetchError(custom_msg)

    column_names = ["geo_id", "val",
                    "se", "sample_size", "time_value"]

    # Rename and reorder columns to match those in df_to_test.
    api_df = api_df.rename(
        columns={'geo_value': "geo_id", 'stderr': 'se', 'value': 'val'}
    ).drop(
        ['issue', 'lag'], axis=1
    ).reindex(columns=column_names)
    # Replace None with NA to make numerical manipulation easier.  We omit the `geo_id` column
    # since sometimes this replacement will covert the strings to numeric values.
    api_df[column_names[1:]] = api_df[column_names[1:]].replace(to_replace=[None], value=np.nan)

    return api_df


def get_one_api_df(data_source, min_date, max_date,
                    geo_type, signal_type,
                    api_semaphore, dict_lock, output_dict):
    """
    Pull API data for a single geo type-signal combination.

    Raises error if data couldn't be retrieved. Saves data to data dict.
    """
    api_semaphore.acquire()

    # Pull reference data from API for all dates.
    try:
        geo_sig_api_df_or_error = fetch_api_reference(
            data_source, min_date, max_date, geo_type, signal_type)

    except APIDataFetchError as e:
        geo_sig_api_df_or_error = ValidationFailure("api_data_fetch_error",
                                                    geo_type=geo_type,
                                                    signal=signal_type,
                                                    message=e.custom_msg)

    api_semaphore.release()

    # Use a lock so only one thread can access the dictionary.
    dict_lock.acquire()
    output_dict[(geo_type, signal_type)] = geo_sig_api_df_or_error
    dict_lock.release()


def threaded_api_calls(data_source, min_date, max_date, geo_signal_combos, n_threads=32):
    """Get data from API for all geo-signal combinations in a threaded way."""
    if n_threads > 32:
        n_threads = 32
        print("Warning: Don't run more than 32 threads at once due "
                + "to API resource limitations")

    output_dict = dict()
    dict_lock = threading.Lock()
    api_semaphore = threading.Semaphore(value=n_threads)

    thread_objs = [threading.Thread(
        target=get_one_api_df, args=(data_source, min_date, max_date,
                                     geo_type, signal_type,
                                     api_semaphore,
                                     dict_lock, output_dict)
    ) for geo_type, signal_type in geo_signal_combos]

    # Start all threads.
    for thread in thread_objs:
        thread.start()

    # Wait until all threads are finished.
    for thread in thread_objs:
        thread.join()

    return output_dict
