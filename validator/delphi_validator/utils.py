"""Utility functions for validation."""
from datetime import datetime
import pandas as pd

# Recognized geo types.
GEO_REGEX_DICT = {
    'county': r'^\d{5}$',
    'hrr': r'^\d{1,3}$',
    'msa': r'^\d{5}$',
    'dma': r'^\d{3}$',
    'state': r'^[a-zA-Z]{2}$',
    'national': r'^[a-zA-Z]{2}$'
}


def relative_difference_by_min(x, y):
    """
    Calculate relative difference between two numbers.
    """
    return (x - y) / min(x, y)


def aggregate_frames(frames_list):
    """Aggregates a list of data frames into a single frame.

    Parameters
    ----------
    frames_list: List[Tuple(str, re.match, pd.DataFrame)]
        triples of filenames, filename matches with the geo regex, and the data from the file

    Returns
    -------
    A pd.DataFrame concatenation of all data frames in `frames_list` with additional columns for
    geo_type, time_value, and signal derived from the corresponding re.match.
    """
    all_frames = []
    for _, match, data_df in frames_list:
        df = data_df.copy()
        # Get geo_type, date, and signal name as specified by CSV name.
        df['geo_type'] = match.groupdict()['geo_type']
        df['time_value'] = datetime.strptime(
            match.groupdict()['date'], "%Y%m%d").date()
        df['signal'] = match.groupdict()['signal']

        all_frames.append(df)

    return pd.concat(all_frames).reset_index(drop=True)
