from datetime import date

import numpy as np

from delphi_nowcast.sensorization.regression_model import compute_regression_sensor
from delphi_nowcast.data_containers import LocationSeries


class TestComputeRegressionSensor:

    def test_compute_regression_sensor_intercept(self):
        """Verified with lm(y~x)."""
        test_covariate = LocationSeries(values=[1, 3, 5, 6, 7, 9, 12],
                                        dates=[date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3),
                                               date(2020, 1, 4), date(2020, 1, 5), date(2020, 1, 6),
                                               date(2020, 1, 7)])
        test_response = LocationSeries(values=[10, 16, 22, 29, 28, 35, 42],
                                       dates=[date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3),
                                              date(2020, 1, 4), date(2020, 1, 5), date(2020, 1, 6),
                                              date(2020, 1, 7)])
        assert np.isclose(
           compute_regression_sensor(date(2020, 1, 6), test_covariate, test_response, True),
           6.586207 + 3.275862 * 9
        )

    def test_compute_regression_sensor_no_intercept(self):
        """Verified with lm(y~x-1)."""
        test_covariate = LocationSeries(values=[1, 3, 5, 6, 7, 9, 12],
                                        dates=[date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3),
                                               date(2020, 1, 4), date(2020, 1, 5), date(2020, 1, 6),
                                               date(2020, 1, 7)])
        test_response = LocationSeries(values=[10, 16, 22, 29, 28, 35, 42],
                                       dates=[date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3),
                                              date(2020, 1, 4), date(2020, 1, 5), date(2020, 1, 6),
                                              date(2020, 1, 7)])
        assert np.isclose(
            compute_regression_sensor(date(2020, 1, 6), test_covariate, test_response, False),
            4.483333 * 9
        )

    def test_compute_regression_sensor_insufficient_data(self):
        test_covariate = LocationSeries(values=[1, 3, np.nan, 6, 7, 9, 12],
                                        dates=[date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3),
                                               date(2020, 1, 4), date(2020, 1, 5), date(2020, 1, 6),
                                               date(2020, 1, 7)])
        test_response = LocationSeries(values=[10, 16, 22, 29, 28, 35, 42],
                                       dates=[date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3),
                                              date(2020, 1, 4), date(2020, 1, 5), date(2020, 1, 6),
                                              date(2020, 1, 7)])
        assert np.isnan(compute_regression_sensor(date(2020, 1, 1), test_covariate, test_response))
        assert np.isnan(compute_regression_sensor(date(2020, 1, 6), test_covariate, test_response))

    def test_compute_regression_sensor_out_of_range(self):
        test_covariate = LocationSeries(values=[1, 3, np.nan, 6, 7, 9, 12],
                                        dates=[date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3),
                                               date(2020, 1, 4), date(2020, 1, 5), date(2020, 1, 6),
                                               date(2020, 1, 7)])
        test_response = LocationSeries(values=[10, 16, 22, 29, 28, 35, 42],
                                       dates=[date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3),
                                              date(2020, 1, 4), date(2020, 1, 5), date(2020, 1, 6),
                                              date(2020, 1, 7)])
        assert np.isnan(compute_regression_sensor(date(2020, 1, 16), test_covariate, test_response))