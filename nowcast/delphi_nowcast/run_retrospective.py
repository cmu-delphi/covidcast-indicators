from datetime import date, timedelta
from functools import partial
from typing import List

import numpy as np
import pandas as pd
from delphi_nowcast.data_containers import SensorConfig
from delphi_nowcast.deconvolution import delay_kernel, deconvolution
from delphi_utils import GeoMapper

FIRST_DATA_DATE = date(2020, 7, 1)  # first date of historical data to use


def run_retrospective(state_geo: str,
                      pred_date: date,
                      as_of: date):
    """Run retrospective nowcasting experiment.

    Parameters
    ----------
    state_geo
        string geo_id of the USA state. counties within the state will be auto-queried.
    pred_date
        date to produce prediction
    as_of
        date that the data should be retrieved as of

    Returns
    -------

    """
    # get list of counties in state and population weights
    gmpr = GeoMapper()
    geo_info = pd.DataFrame({"fips": sorted(list(gmpr.get_geo_values("fips")))})
    geo_info = gmpr.add_geocode(geo_info, "fips", "state", "fips", "state")
    geo_info = geo_info[geo_info.state_id.eq(state_geo)]
    geo_info = gmpr.add_population_column(geo_info, "fips")
    state = geo_info[geo_info.fips.str.endswith("000")]
    fips = geo_info[~geo_info.fips.str.endswith("000")]

    # define locations
    input_locations = [(fips_geo, 'county') for fips_geo in fips.fips]
    input_locations.append((state_geo, 'state'))

    # define signals
    regression_indicators = [
        SensorConfig('usa-facts', 'confirmed_incidence_num', 'ar3', 1),
        SensorConfig('fb-survey', 'smoothed_hh_cmnty_cli', 'fb', 3)
    ]

    convolved_truth_indicator = SensorConfig('usa-facts', 'confirmed_cumulative_prop',
                                             'test_truth', 0)

    sensor_indicators = [convolved_truth_indicator] + regression_indicators

    # get deconvolved ground truth
    kernel, delay_coefs = delay_kernel.get_florida_delay_distribution()  # param-to-store: delay_coefs
    cv_grid = np.logspace(1, 3.5, 20)  # param-to-store
    n_cv_folds = 10  # param-to-store
    deconvolve_func = partial(deconvolution.deconvolve_tf_cv,
                              cv_grid=cv_grid, n_folds=n_cv_folds)

    ground_truth = deconvolution.deconvolve_signal(convolved_truth_indicator,
                                                   FIRST_DATA_DATE,
                                                   pred_date - timedelta(days=1),
                                                   as_of,
                                                   input_locations,
                                                   np.array(kernel),
                                                   deconvolve_func)

    ## compute most recent sensor vector
    ## run sensor fusion
    ## return output
    pass


def run_retrospective_batch(date_range: List[date]):
    pass
