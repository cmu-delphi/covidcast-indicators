from datetime import date, timedelta
from functools import partial
from typing import Dict, List, Tuple

import numpy as np

from delphi_nowcast.data_containers import SensorConfig
from delphi_nowcast.deconvolution import delay_kernel, deconvolution
from delphi_nowcast.sensorization import sensor


# todo: add scipy to Makefile

def compute_batch_sensors(input_locations: List[Tuple[str, str]],
                          as_of_dates: List[date],
                          export_dir: str,
                          first_data_date: date = date(2020, 7, 1)) -> Dict[date, Dict]:
    """
    Compute batch of historical sensor values.

    Parameters
    ----------
    input_locations
        locations for which to compute sensors
    as_of_dates
        list of dates that data should be retrieved as of
    export_dir
        directory path to store sensor csv
    first_data_date
        first date of historical data to use

    Returns
    -------
        Dict where keys are the as_of date and values are the Dict returned from
        sensor::compute_sensors().
    """
    # define signals
    regression_indicators = [
        SensorConfig('usa-facts', 'confirmed_incidence_num', 'ar3', 1),
        SensorConfig('fb-survey', 'smoothed_hh_cmnty_cli', 'fb', 3)
    ]

    convolved_truth_indicator = SensorConfig(
        'usa-facts', 'confirmed_cumulative_prop', 'test_truth', 0)

    # get deconvolved ground truth
    kernel, delay_coefs = delay_kernel.get_florida_delay_distribution()  # param-to-store: delay_coefs
    cv_grid = np.logspace(1, 3.5, 20)  # param-to-store
    n_cv_folds = 10  # param-to-store
    deconvolve_func = partial(deconvolution.deconvolve_tf_cv,
                              cv_grid=cv_grid, n_folds=n_cv_folds)

    out_sensors = {}
    for as_of in as_of_dates:
        ground_truth = deconvolution.deconvolve_signal(convolved_truth_indicator,
                                                       first_data_date,
                                                       as_of - timedelta(days=1),
                                                       as_of,
                                                       input_locations,
                                                       np.array(kernel),
                                                       deconvolve_func)

        out_sensors[as_of] = sensor.compute_sensors(as_of,
                                                    regression_indicators,
                                                    convolved_truth_indicator,
                                                    ground_truth,
                                                    export_dir)

    return out_sensors
