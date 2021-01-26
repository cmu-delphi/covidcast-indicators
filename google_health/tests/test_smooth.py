import numpy as np
import pandas as pd

from delphi_google_health.smooth import smoothed_values_by_geo_id, _left_gauss_linear


class TestSmoothedValues:
    def test_smooth(self):

        df = pd.DataFrame(
            {
                "geo_id": ["a", "a", "a", "a", "b", "b", "b"],
                "timestamp": [
                    "2020-02-01",
                    "2020-02-02",
                    "2020-02-03",
                    "2020-02-04",
                    "2020-02-01",
                    "2020-02-02",
                    "2020-02-03",
                ],
                "val": np.array([0, 1, 2, 2, 1, 3, 7]),
            }
        )

        smoothed = smoothed_values_by_geo_id(df)
        direct_call = np.append(
            _left_gauss_linear(df["val"][0:4].values, impute=True, minval=0),
            _left_gauss_linear(df["val"][4:7].values, impute=True, minval=0),
        )

        assert np.allclose(smoothed, direct_call)


class TestSmoother:
    def test_gauss_linear(self):

        signal = np.ones(10)

        assert np.allclose(_left_gauss_linear(signal, impute=True), signal)
        assert np.all(
            _left_gauss_linear(signal, impute=True, minval=2) == 2 * np.ones(10)
        )

        signal = np.arange(10)
        assert np.allclose(_left_gauss_linear(signal, impute=True), signal)
