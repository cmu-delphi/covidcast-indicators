from unittest.mock import patch
from datetime import date

import numpy as np

from delphi_nowcast.sensorization.ar_model import compute_ar_sensor
from delphi_nowcast.data_containers import LocationSeries


class TestComputeARSensor:

    @patch("numpy.random.normal")
    def test_compute_ar_sensor_no_regularize(self, random_normal):
        """Verified with ar.ols(x, FALSE, ar_size, intercept=TRUE, demean=FALSE)."""
        random_normal.return_value = 0
        values = LocationSeries(
            values=[-4.27815483, -4.83962077, -4.09548122, -3.86647783, -2.64494168, -3.99573135,
                    -3.48248410, -2.77490127, -3.64162355, -2.57628910, -2.46793048, -3.20454941,
                    -1.77057154, -0.02058535, 0.81182691, 0.32741982],
            dates=[date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3), date(2020, 1, 4),
                   date(2020, 1, 5), date(2020, 1, 6), date(2020, 1, 7), date(2020, 1, 8),
                   date(2020, 1, 9), date(2020, 1, 10), date(2020, 1, 11), date(2020, 1, 12),
                   date(2020, 1, 13), date(2020, 1, 14), date(2020, 1, 15), date(2020, 1, 16)])
        assert np.isclose(
            compute_ar_sensor(date(2020, 1, 15), values, 1, 0),
            -0.09105891 + 0.87530957 * -0.02058535
        )
        assert np.isclose(
            compute_ar_sensor(date(2020, 1, 15), values, 2, 0),
            0.31865395 + 0.64751725 * -0.02058535 + 0.30760218 * -1.77057154
        )

    @patch("numpy.random.normal")
    def test_compute_ar_sensor_regularize(self, random_normal):
        """coefficients verified with lm.ridge(y~x1+x2, lambda=1*12/11)

        x1 and x2 constructed by hand, lambda is scaled since lm.ridge does some scaling by n/(n-1)
        """
        random_normal.return_value = 0
        values = LocationSeries(
            values=[-4.27815483, -4.83962077, -4.09548122, -3.86647783, -2.64494168, -3.99573135,
                    -3.48248410, -2.77490127, -3.64162355, -2.57628910, -2.46793048, -3.20454941,
                    -1.77057154, -0.02058535, 0.81182691, 0.32741982],
            dates=[date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3), date(2020, 1, 4),
                   date(2020, 1, 5), date(2020, 1, 6), date(2020, 1, 7), date(2020, 1, 8),
                   date(2020, 1, 9), date(2020, 1, 10), date(2020, 1, 11), date(2020, 1, 12),
                   date(2020, 1, 13), date(2020, 1, 14), date(2020, 1, 15), date(2020, 1, 16)])

        assert np.isclose(compute_ar_sensor(date(2020, 1, 15), values, 2, 1),
                          -2.8784639 +
                          0.2315984 * (-1.77057154 - -3.48901547)/0.7637391 +
                          0.5143709 * (-0.02058535 - -3.28005019)/0.8645852
                          )

    def test_compute_ar_sensor_seed(self):
        """Test same result over 50 runs"""
        values = LocationSeries(
            values=[-4.27815483, -4.83962077, -4.09548122, -3.86647783, -2.64494168, -3.99573135,
                    -3.48248410, -2.77490127, -3.64162355, -2.57628910, -2.46793048, -3.20454941,
                    -1.77057154, -0.02058535, 0.81182691, 0.32741982],
            dates=[date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3), date(2020, 1, 4),
                   date(2020, 1, 5), date(2020, 1, 6), date(2020, 1, 7), date(2020, 1, 8),
                   date(2020, 1, 9), date(2020, 1, 10), date(2020, 1, 11), date(2020, 1, 12),
                   date(2020, 1, 13), date(2020, 1, 14), date(2020, 1, 15), date(2020, 1, 16)])
        assert len(set(compute_ar_sensor(date(2020, 1, 15), values, 1, 0) for _ in range(50))) == 1

    def test_compute_ar_sensor_insufficient_data(self):
        values = LocationSeries(
            values=[-4.27815483, -4.83962077],
            dates=[date(2020, 1, 1), date(2020, 1, 2)])
        assert np.isnan(compute_ar_sensor(date(2020, 1, 2), values))
        assert np.isnan(compute_ar_sensor(date(2020, 1, 7), values))

    def test_compute_ar_sensor_out_of_range(self):
        values = LocationSeries(
            values=[-4.27815483, -4.83962077],
            dates=[date(2020, 1, 1), date(2020, 1, 2)])
        assert np.isnan(compute_ar_sensor(date(2020, 1, 7), values))
