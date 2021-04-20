"""Functions to construct the ground truth for a given location."""
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
                     export_dir: str = "") -> Dict[Tuple, LocationSeries]:
    raw_indicator = get_indicator_data([truth], locations, as_of)
    output = {}
    for location in locations:
        indicator_key = (truth.source, truth.signal, location.geo_type, location.geo_value)
        location, missing_dates = get_historical_sensor_data(truth, location, end_date, start_date)
        location, export = fill_missing_days(location, raw_indicator[indicator_key], missing_dates)
        output[indicator_key] = location
        if export_dir and export.values:
            export_to_csv(export,  truth, as_of, export_dir)
        return output


def fill_missing_days(stored_vals: LocationSeries,
                      indicator_data: LocationSeries,
                      missing_dates: List[date]):
    export_data = LocationSeries(stored_vals.geo_value, stored_vals.geo_type)
    for day in missing_dates:
        try:
            y = np.array(indicator_data.get_data_range(min(indicator_data.dates), day, "linear"))
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
