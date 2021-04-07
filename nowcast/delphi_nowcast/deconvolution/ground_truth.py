"""Functions to construct the ground truth for training sensors."""
from datetime import date
from typing import List, Dict, Tuple

import numpy as np

from .deconvolution import deconvolve_double_smooth_tf_cv
from ..constants import Default
from ..data_containers import LocationSeries, SensorConfig
from ..epidata import get_historical_sensor_data, get_indicator_data, export_to_csv


def construct_truths(start_date: date,
                     end_date: date,
                     as_of: date,  # most likely today
                     truth: SensorConfig,
                     locations: List[LocationSeries],
                     export_dir: str = "") -> Dict[SensorConfig, List[LocationSeries]]:
    """
    Construct ground truths for use in training sensors.

    Parameters
    ----------
    start_date
        First day to get truths for.
    end_date
        Last day to get truths for.
    as_of
        Day to get data as of for computing new values.
    truth
        SensorConfig of previously stored truths.
    locations
        List of LocationSeries containing geo info to get truths for.
    export_dir


    Returns
    -------
        Dictionary where key is `truth` and values are populated LocationSeries from `location`
    """
    raw_indicator = get_indicator_data([truth], locations, as_of)
    output = {truth: []}
    for location in locations:
        indicator_key = (truth.source, truth.signal, location.geo_type, location.geo_value)
        location, missing_dates = get_historical_sensor_data(truth, location, end_date, start_date)
        if indicator_key in raw_indicator:
            indicator_loc_data = raw_indicator[indicator_key]
            location, export = fill_missing_days(location, indicator_loc_data, missing_dates)
        else:
            export = None
        output[truth].append(location)
        if export_dir and export:
            print(export_to_csv(export,  truth, as_of, export_dir))
    return output


def fill_missing_days(stored_vals: LocationSeries,
                      indicator_data: LocationSeries,
                      missing_dates: List[date]) -> Tuple[LocationSeries, LocationSeries]:
    """
    Compute and add deconvolved truths to a LocationSeries for a set of missing days.

    If a missing day cannot be filled, it will be skipped over.

    Parameters
    ----------
    stored_vals
        LocationSeries containing data with missing dates.
    indicator_data
        LocationSeries of convolved data from indicator.
    missing_dates
        List of missing dates to fill.

    Returns
    -------
        `stored_vals` with missing dates filled if possible.
    """
    export_data = LocationSeries(stored_vals.geo_value, stored_vals.geo_type)
    for day in missing_dates:
        try:
            y = np.array(indicator_data.get_data_range(min(indicator_data.dates), day, "mean"))
            x = np.arange(1, len(y) + 1)
        except ValueError:
            continue
        deconv_vals = deconvolve_double_smooth_tf_cv(
            y, x, Default.DELAY_DISTRIBUTION
        )
        missing_day_val = deconv_vals[-1]
        stored_vals.add_data(day, missing_day_val)
        export_data.add_data(day, missing_day_val)  # holds only data to get exported
    return stored_vals, export_data
