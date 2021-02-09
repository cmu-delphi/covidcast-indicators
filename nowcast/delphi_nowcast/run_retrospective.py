from datetime import date, timedelta
from typing import List

import numpy as np
import pandas as pd

from delphi_nowcast.constants import Default
from delphi_nowcast.deconvolution import deconvolution
from delphi_nowcast.nowcast_fusion import covariance, fusion
from delphi_nowcast.sensorization import sensor
from delphi_nowcast.statespace import statespace
from delphi_utils import GeoMapper


def run_retrospective(state_id: str,
                      pred_date: date,
                      as_of: date,
                      export_dir: str,
                      first_data_date):
    """Run retrospective nowcasting experiment.

    Parameters
    ----------
    state_id
        string geo_id of the USA state. counties within the state will be auto-queried.
    pred_date
        date to produce prediction
    as_of
        date that the data should be retrieved as of
    export_dir
        directory path to store create sensor csv files
    first_data_date
        first date of historical data to use

    Returns
    -------

    """
    # set to default config
    regression_indicators = Default.REG_SENSORS
    convolved_truth_indicator = Default.GROUND_TRUTH_INDICATOR
    kernel = Default.DELAY_DISTRIBUTION
    deconvolve_func = Default.DECONV_FIT_FUNC

    # get list of counties in state and population weights
    pop_df = statespace.get_fips_in_state_pop_df(state_id)

    # define locations
    input_locations = [(fips_geo, 'county') for fips_geo in pop_df.fips]
    input_locations.append((state_id, 'state'))

    # --- get deconvolved ground truth ---
    ground_truth = deconvolution.deconvolve_signal(
        convolved_truth_indicator,
        first_data_date,
        pred_date - timedelta(convolved_truth_indicator.lag),
        as_of,
        input_locations,
        np.array(kernel),
        deconvolve_func)

    # --- compute most recent sensor vector ---
    pred_sensors = sensor.compute_sensors(
        as_of,
        regression_indicators,
        convolved_truth_indicator,
        ground_truth,
        export_dir)

    # --- get historical sensors ---
    sensor_indicators = [convolved_truth_indicator] + regression_indicators
    hist_sensors = sensor.historical_sensors(
        first_data_date,
        pred_date - timedelta(convolved_truth_indicator.lag),
        sensor_indicators,
        ground_truth)

    # --- run sensor fusion ---
    # move to matrix form
    n_dates = (pred_date - first_data_date).days
    input_dates = [first_data_date + timedelta(days=a) for a in range(n_dates)]
    y = dict(((s.geo_value, s.geo_type), s) for s in ground_truth)
    n_sensor_locs = len(sensor_indicators) * len(input_locations)
    noise = np.full((len(input_dates), n_sensor_locs), np.nan)
    z = np.full((1, n_sensor_locs), np.nan)
    valid_location_types = []
    j = 0
    for sensor_config in sensor_indicators:
        # convert to dict indexed by loc to make matching across train/test easier
        train_series = dict(
            ((s.geo_value, s.geo_type), s) for s in hist_sensors[sensor_config])
        test_series = dict(
            ((s.geo_value, s.geo_type), s) for s in pred_sensors[sensor_config])
        valid_locs = set(train_series.keys()) & set(test_series.keys())

        for loc in sorted(valid_locs):
            dates_intersect = sorted(set(y[loc].dates) & set(train_series[loc].dates))
            y_vals = y[loc].get_data_range(
                dates_intersect[0], dates_intersect[-1])
            s_vals = train_series[loc].get_data_range(
                dates_intersect[0], dates_intersect[-1])

            inds = [i for i, date in enumerate(dates_intersect) if date in input_dates]
            noise[inds, j] = np.array(y_vals) - np.array(s_vals)
            z[:, j] = test_series[loc].values
            valid_location_types.append(loc)
            j += 1

    # cull nan columns
    finite_cols = np.logical_and(np.any(np.isfinite(noise), axis=0),
                                 np.all(np.isfinite(z), axis=0))

    noise = noise[:, finite_cols]
    z = z[:, finite_cols]

    # determine statespace
    H, W, output_locations = statespace.generate_statespace(
        state_id, valid_location_types, pop_df)

    # estimate covariance
    R = covariance.mle_cov(noise, covariance.BlendDiagonal2)

    # run SF
    x, P = fusion.fuse(z, R, H)
    y, S = fusion.extract(x, P, W)
    stdev = np.sqrt(np.diag(S)).reshape(y.shape)

    return y, stdev, output_locations


def run_batch_retrospective(date_range: List[date]):
    pass
