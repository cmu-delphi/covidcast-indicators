from datetime import date

import numpy as np

from delphi_nowcast.sensorization.regression_model import compute_regression_sensor
from delphi_nowcast.data_containers import LocationSeries


class TestComputeRegressionSensor:

    def test_compute_regression_sensor_intercept(self):
        """Verified with lm(y~x)."""
        test_covariate = LocationSeries(
            data={date(2020, 1, 1): 1, date(2020, 1, 2): 3, date(2020, 1, 3): 5,
                  date(2020, 1, 4): 6, date(2020, 1, 5): 7, date(2020, 1, 6): 9,
                  date(2020, 1, 7): 12}
        )
        test_response = LocationSeries(
            data={date(2020, 1, 1): 10, date(2020, 1, 2): 16, date(2020, 1, 3): 22,
                  date(2020, 1, 4): 29, date(2020, 1, 5): 28, date(2020, 1, 6): 35,
                  date(2020, 1, 7): 42}
        )
        assert np.isclose(
           compute_regression_sensor(date(2020, 1, 6), test_covariate, test_response, True),
           6.586207 + 3.275862 * 9
        )

    def test_compute_regression_sensor_no_intercept(self):
        """Verified with lm(y~x-1)."""
        test_covariate = LocationSeries(
            data={date(2020, 1, 1): 1, date(2020, 1, 2): 3, date(2020, 1, 3): 5,
                  date(2020, 1, 4): 6, date(2020, 1, 5): 7, date(2020, 1, 6): 9,
                  date(2020, 1, 7): 12}
        )
        test_response = LocationSeries(
            data={date(2020, 1, 1): 10, date(2020, 1, 2): 16, date(2020, 1, 3): 22,
                  date(2020, 1, 4): 29, date(2020, 1, 5): 28, date(2020, 1, 6): 35,
                  date(2020, 1, 7): 42}
        )
        assert np.isclose(
            compute_regression_sensor(date(2020, 1, 6), test_covariate, test_response, False),
            4.483333 * 9
        )

    def test_compute_regression_sensor_insufficient_data(self):
        test_covariate = LocationSeries(
            data={date(2020, 1, 1): 1, date(2020, 1, 2): 3, date(2020, 1, 3): np.nan,
                  date(2020, 1, 4): 6, date(2020, 1, 5): 7, date(2020, 1, 6): 9,
                  date(2020, 1, 7): 12}
        )
        test_response = LocationSeries(
            data={date(2020, 1, 1): 10, date(2020, 1, 2): 16, date(2020, 1, 3): 22,
                  date(2020, 1, 4): 29, date(2020, 1, 5): 28, date(2020, 1, 6): 35,
                  date(2020, 1, 7): 42}
        )
        assert np.isnan(compute_regression_sensor(date(2020, 1, 1), test_covariate, test_response, False))
        assert np.isnan(compute_regression_sensor(date(2020, 1, 6), test_covariate, test_response, False))

    def test_compute_regression_sensor_out_of_range(self):
        test_covariate = LocationSeries(
            data={date(2020, 1, 1): 1, date(2020, 1, 2): 3, date(2020, 1, 3): 5,
                  date(2020, 1, 4): 6, date(2020, 1, 5): 7, date(2020, 1, 6): 9,
                  date(2020, 1, 7): 12}
        )
        test_response = LocationSeries(
            data={date(2020, 1, 1): 10, date(2020, 1, 2): 16, date(2020, 1, 3): 22,
                  date(2020, 1, 4): 29, date(2020, 1, 5): 28, date(2020, 1, 6): 35,
                  date(2020, 1, 7): 42}
        )
        assert np.isnan(compute_regression_sensor(date(2020, 1, 16), test_covariate, test_response, False))

    def test_compute_regression_sensor_no_data(self):
        test_covariate = LocationSeries()
        test_response = LocationSeries()
        assert np.isnan(compute_regression_sensor(date(2020, 1, 16), test_covariate, test_response, False))
