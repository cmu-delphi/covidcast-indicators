import os
from os import listdir
from os.path import isfile, join, basename
import glob
from datetime import date
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import covidcast
from joblib import Memory

ROOT_DIR = os.getcwd()
CACHE_DIR = join(ROOT_DIR, "testing_utils", "cache")
memory = Memory(CACHE_DIR, verbose=0)


def read_relevant_date_filenames(
    data_path, date_slist, geo_code="", signal_type="", return_path=False
):
    """
    Return a list of tuples of every filename in the specified directory if the file is in the specified date range.
    Arguments:
        - data_path: path to the directory containing CSV data files.
        - date_slist: list of dates (formatted as strings) to check
        - geo_code: a string containing a geocode to expect in the filename
        - signal_type: a string containing the name of the signal to expect in the filename
        - return_path: if True, returns the full path to the file, else returns just the filename
    Returns:
        - list
    """
    all_files = [f for f in listdir(data_path) if isfile(join(data_path, f))]
    filelist = list()

    for fl in all_files:
        for dt in date_slist:
            if fl.find(dt) != -1 and geo_code in fl and signal_type in fl:
                filelist.append(fl if not return_path else join(data_path, fl))

    return filelist


def generate_date_range(start, end):
    """
    Return a list of string-formatted dates in the format YYYYMMDD from start to end, both ends inclusive.

    Parameters
    ----------
    start: str
        The start date in the same string format as above.
    end: str
        The end date in the same string format as above.

    Return
    ---------
    List of dates.
    """
    return [x.strftime("%Y%m%d") for x in pd.date_range(start, end)]


def load_csvs(filename_list):
    """
    Returns a list of dataframes for each csv file in the list. Each dataframe is appended with a date column
    that contains the date in the first 8 characters of the filename with the format YYYYMMDD.
    """
    all_frames = []
    for filename in filename_list:
        frame = pd.read_csv(
            join("receiving", filename),
            dtype={"geo_id": str, "val": float, "se": float, "sample_size": float},
        )
        frame["date"] = datetime(
            int(basename(filename)[:4]),
            int(basename(filename)[4:6]),
            int(basename(filename)[6:8]),
        )
        all_frames.append(frame)
    return all_frames

@memory.cache
def load_local_signal_data(local_signal_dir, signal_type, start_day, end_day, geo_type):
    """
    This function loads a local directory's .csv files into a single timestamped dataframe for analysis.

    This function is cached to disk to speed up computation times. You can force clear the cache like so:
    >>> load_local_signal_data.clear()
    """
    date_range = generate_date_range(
        start_day.strftime("%Y%m%d"), end_day.strftime("%Y%m%d")
    )
    local_files = read_relevant_date_filenames(
        join(ROOT_DIR, local_signal_dir, "receiving"), date_range, geo_type, signal_type, return_path=True
    )
    local_data = pd.concat(load_csvs(local_files)).set_index(["geo_id", "date"]).sort_index()
    
    return local_data

@memory.cache
def load_remote_signal_data(remote_source_name, signal_type, start_day, end_day, geo_type):
    """
    This function is a caching wrapper for the covidcast signal function. You can force clear the cache like so:
    >>> load_remote_signal_data.clear()
    """
    remote_data = covidcast.signal(
            remote_source_name, signal_type, start_day, end_day, geo_type
        )
    if not isinstance(remote_data, type(None)):
        remote_data["time_value"] = pd.to_datetime(remote_data["time_value"])
        remote_data["geo_value"] = remote_data["geo_value"].astype(str)
        remote_data = remote_data.set_index(["geo_value", "time_value"]).sort_index()

    return remote_data

def load_signal_data(
    local_signal_dir, remote_source_name, signal_type, start_day, end_day, geo_type
):
    """
    This function loads two dataframes: local_data and remote_data. The local_data is stitched together from the .csv
    files contained in your local_signal_dir. The remote_data is pulled from covidcast remote_source_name.

    The intermediate results are cached to disk, which can be cleared like so:
    >>> load_local_signal_data.clear()
    >>> load_remote_signal_data.clear()
    """
    local_data = load_local_signal_data(local_signal_dir, signal_type, start_day, end_day, geo_type)
    remote_data = load_remote_signal_data(remote_source_name, signal_type, start_day, end_day, geo_type)

    return local_data, remote_data
