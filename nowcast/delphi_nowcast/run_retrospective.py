from datetime import date, timedelta
from functools import partial
from typing import List

import numpy as np
import pandas as pd

from delphi_nowcast.data_containers import SensorConfig
from delphi_nowcast.deconvolution import delay_kernel, deconvolution
from delphi_nowcast.sensorization import sensor
from delphi_utils import GeoMapper


def run_retrospective(state_id: str,
                      pred_date: date,
                      as_of: date,
                      export_dir: str,
                      first_data_date: date = date(2020, 7, 1)):
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
    # get list of counties in state and population weights
    gmpr = GeoMapper()
    geo_info = pd.DataFrame({"fips": sorted(list(gmpr.get_geo_values("fips")))})
    geo_info = gmpr.add_geocode(geo_info, "fips", "state", "fips", "state")
    geo_info = geo_info[geo_info.state_id.eq(state_id)]
    geo_info = gmpr.add_population_column(geo_info, "fips")
    state = geo_info[geo_info.fips.str.endswith("000")]
    fips = geo_info[~geo_info.fips.str.endswith("000")]

    # define locations
    input_locations = [(fips_geo, 'county') for fips_geo in fips.fips]
    input_locations.append((state_id, 'state'))

    # define signals
    regression_indicators = [
        SensorConfig('usa-facts', 'confirmed_incidence_num', 'ar3', 1),
        SensorConfig('fb-survey', 'smoothed_hh_cmnty_cli', 'fb', 3)
    ]

    convolved_truth_indicator = SensorConfig('usa-facts', 'confirmed_cumulative_prop',
                                             'test_truth', 0)

    sensor_indicators = [convolved_truth_indicator] + regression_indicators

    kernel, delay_coefs = delay_kernel.get_florida_delay_distribution()  # param-to-store: delay_coefs
    cv_grid = np.logspace(1, 3.5, 20)  # param-to-store
    n_cv_folds = 10  # param-to-store
    deconvolve_func = partial(deconvolution.deconvolve_tf_cv,
                              cv_grid=cv_grid, n_folds=n_cv_folds)

    # --- get deconvolved ground truth ---
    ground_truth = deconvolution.deconvolve_signal(convolved_truth_indicator,
                                                   first_data_date,
                                                   pred_date - timedelta(days=1),
                                                   as_of,
                                                   input_locations,
                                                   np.array(kernel),
                                                   deconvolve_func)

    # --- compute most recent sensor vector ---
    pred_sensors = sensor.compute_sensors(as_of,
                                          regression_indicators,
                                          convolved_truth_indicator,
                                          ground_truth,
                                          export_dir)

    # --- get historical sensors ---
    hist_sensors = sensor.historical_sensors(first_data_date,
                                             pred_date - timedelta(days=1),
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
            inds = [i for i, date in enumerate(dates_intersect) if date in input_dates]
            y_vals = y[loc].get_data_range(dates_intersect[0], dates_intersect[-1])
            s_vals = train_series[loc].get_data_range(dates_intersect[0],
                                                      dates_intersect[-1])
            noise[inds, j] = np.array(y_vals) - np.array(s_vals)
            z[:, j] = test_series[loc].values
            valid_location_types.append(loc)
            j += 1

    ## todo: adjust statespace code
    ## return output
    pass


def run_batch_retrospective(date_range: List[date]):
    pass
