from datetime import date, timedelta
from typing import Dict, List, Tuple

import numpy as np

from delphi_nowcast.constants import Default
from delphi_nowcast.deconvolution import deconvolution
from delphi_nowcast.sensorization import sensor


def compute_batch_sensors(input_locations: List[Tuple[str, str]],
                          as_of_dates: List[date],
                          export_dir: str,
                          first_data_date: date) -> Dict[date, Dict]:
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

    # set to default config
    regression_indicators = Default.REG_SENSORS
    convolved_truth_indicator = Default.GROUND_TRUTH_INDICATOR
    kernel = Default.DELAY_DISTRIBUTION
    deconvolve_func = Default.DECONV_FIT_FUNC

    out_sensors = {}
    for as_of in as_of_dates:
        ground_truth = deconvolution.deconvolve_signal(
            convolved_truth_indicator,
            first_data_date,
            # +1 since we only need truths up to to the day before the desired date for training
            as_of - timedelta(convolved_truth_indicator.lag + 1),
            as_of,
            input_locations,
            np.array(kernel),
            deconvolve_func)
        out_sensors[as_of] = sensor.compute_sensors(
            as_of,
            regression_indicators,
            convolved_truth_indicator,
            ground_truth,
            export_dir)

    return out_sensors
