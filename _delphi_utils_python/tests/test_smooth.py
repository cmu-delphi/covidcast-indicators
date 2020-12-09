"""
Tests for the smoothing utility.
Authors: Dmitry Shemetov, Addison Hu, Maria Jahja
"""
import pytest

import numpy as np
import pandas as pd
from delphi_utils import Smoother


class TestSmoothers:
    def test_identity_smoother(self):
        signal = np.arange(30) + np.random.rand(30)
        assert np.allclose(signal, Smoother(smoother_name="identity").smooth(signal))

    def test_moving_average_smoother(self):
        # The raw and smoothed lengths should match
        signal = np.ones(30)
        smoother = Smoother(smoother_name="moving_average")
        smoothed_signal = smoother.smooth(signal)
        assert len(signal) == len(smoothed_signal)

        # The raw and smoothed arrays should be identical on constant data
        # modulo the nans
        signal = np.ones(30)
        window_length = 10
        smoother = Smoother(smoother_name="moving_average", window_length=window_length)
        smoothed_signal = smoother.smooth(signal)
        assert np.allclose(
            signal[window_length - 1 :], smoothed_signal[window_length - 1 :]
        )

    def test_left_gauss_linear_smoother(self):
        # The raw and smoothed lengths should match
        signal = np.ones(30)
        smoother = Smoother(smoother_name="left_gauss_linear")
        smoothed_signal = smoother.smooth(signal)
        assert len(signal) == len(smoothed_signal)
        # The raw and smoothed arrays should be identical on constant data
        # modulo the nans
        assert np.allclose(signal[1:], smoothed_signal[1:])

        # The smoother should basically be the identity when the Gaussian kernel
        # is set to weigh the present value overwhelmingly
        signal = np.arange(1, 30) + np.random.normal(0, 1, 29)
        smoother = Smoother(smoother_name="left_gauss_linear", gaussian_bandwidth=0.1)
        assert np.allclose(smoother.smooth(signal)[1:], signal[1:])

    def test_causal_savgol_coeffs(self):
        # The coefficients should return standard average weights for M=0
        nl, nr = -10, 0
        window_length = nr - nl + 1
        smoother = Smoother(
            smoother_name="savgol",
            window_length=window_length,
            poly_fit_degree=0,
            gaussian_bandwidth=None,
        )
        assert np.allclose(smoother.coeffs, np.ones(window_length) / window_length)

    def test_causal_savgol_smoother(self):
        # The raw and smoothed lengths should match
        signal = np.ones(30)
        window_length = 10
        smoother = Smoother(
            smoother_name="savgol", window_length=window_length, poly_fit_degree=0
        )
        smoothed_signal = smoother.smooth(signal)
        assert len(signal) == len(smoothed_signal)
        # The raw and smoothed arrays should be identical on constant data
        # modulo the nans, when M >= 0
        assert np.allclose(
            signal[window_length - 1 :], smoothed_signal[window_length - 1 :]
        )

        # The raw and smoothed arrays should be identical on linear data
        # modulo the nans, when M >= 1
        signal = np.arange(30)
        smoother = Smoother(
            smoother_name="savgol", window_length=window_length, poly_fit_degree=1
        )
        smoothed_signal = smoother.smooth(signal)
        assert np.allclose(
            signal[window_length - 1 :], smoothed_signal[window_length - 1 :]
        )

        # The raw and smoothed arrays should be identical on quadratic data
        # modulo the nans, when M >= 2
        signal = np.arange(30) ** 2
        smoother = Smoother(
            smoother_name="savgol", window_length=window_length, poly_fit_degree=2
        )
        smoothed_signal = smoother.smooth(signal)
        assert np.allclose(
            signal[window_length - 1 :], smoothed_signal[window_length - 1 :]
        )

        # The savgol method should match the linear regression method on the first
        # window_length-many values of the signal, if the savgol_weighting is set to true,
        # and the polynomial fit degree is set to 1. Beyond that, there will be very small
        # differences between the signals (due to "left_gauss_linear" not having a window_length
        # cutoff).
        window_length = 50
        signal = np.arange(window_length) + np.random.randn(window_length)
        smoother = Smoother(smoother_name="left_gauss_linear")
        smoothed_signal1 = smoother.smooth(signal)
        smoother = Smoother(
            smoother_name="savgol", window_length=window_length, poly_fit_degree=1,
        )
        smoothed_signal2 = smoother.smooth(signal)

        assert np.allclose(smoothed_signal1, smoothed_signal2)

    def test_impute(self):
        # test the nan imputer
        signal = np.array([i if i % 3 else np.nan for i in range(1, 40)])
        assert np.allclose(Smoother(impute_method=None).impute(signal), signal, equal_nan=True)

        # test the zeros imputer
        signal = np.array([i if i % 3 else np.nan for i in range(1, 40)])
        assert np.allclose(
            Smoother(impute_method="zeros").impute(signal),
            np.array([i if i % 3 else 0.0 for i in range(1, 40)])
        )

        # make a signal with periodic nans to test the imputer
        signal = np.array([i if i % 3 else np.nan for i in range(1, 40)])
        # test that the non-nan values are unchanged
        not_nans_ixs = np.bitwise_xor(np.isnan(signal, where=True), np.full(len(signal), True))
        smoothed_signal = Smoother().savgol_impute(signal)
        assert np.allclose(signal[not_nans_ixs], smoothed_signal[not_nans_ixs])
        # test that the imputer is close to the true line
        assert np.allclose(range(1, 40), smoothed_signal, atol=0.5)

        # should impute the next value in a linear progression with M>=1
        signal = np.hstack([np.arange(10), [np.nan], np.arange(10)])
        window_length = 10
        smoother = Smoother(
            smoother_name="savgol", window_length=window_length, poly_fit_degree=1
        )
        imputed_signal = smoother.savgol_impute(signal)
        assert np.allclose(imputed_signal, np.hstack([np.arange(11), np.arange(10)]))
        smoother = Smoother(
            smoother_name="savgol", window_length=window_length, poly_fit_degree=2
        )
        imputed_signal = smoother.savgol_impute(signal)
        assert np.allclose(imputed_signal, np.hstack([np.arange(11), np.arange(10)]))

        # if there are nans on the boundary, should dynamically change window
        signal = np.hstack(
            [np.arange(5), [np.nan], np.arange(20), [np.nan], np.arange(5)]
        )
        smoother = Smoother(
            smoother_name="savgol", window_length=window_length, poly_fit_degree=2
        )
        imputed_signal = smoother.savgol_impute(signal)
        assert np.allclose(
            imputed_signal, np.hstack([np.arange(6), np.arange(21), np.arange(5)]),
        )

        # if the array begins with np.nan, we should tell the user to peel it off before sending
        signal = np.hstack([[np.nan], np.arange(20), [np.nan], np.arange(5)])
        smoother = Smoother(
            smoother_name="savgol", window_length=window_length, poly_fit_degree=2
        )
        with pytest.raises(ValueError):
            imputed_signal = smoother.savgol_impute(signal)

        # test window_length > len(signal)
        signal = np.arange(20)
        smoother = Smoother(smoother_name="savgol", boundary_method="identity", window_length=30)
        with pytest.raises(ValueError):
            smoothed_signal = smoother.smooth(signal)

        # test the boundary methods
        signal = np.arange(20)
        smoother = Smoother(smoother_name="savgol", poly_fit_degree=0,
                            boundary_method="identity", window_length=10)
        smoothed_signal = smoother.savgol_impute(signal)
        assert np.allclose(smoothed_signal, signal)

    def test_pandas_series_input(self):
        # The savgol method should match the linear regression method on the first
        # window_length-many values of the signal, if the savgol_weighting is set to true,
        # and the polynomial fit degree is set to 1. Beyond that, there will be very small
        # differences between the signals (due to "left_gauss_linear" not having a window_length
        # cutoff).
        window_length = 50
        signal = pd.Series(np.arange(window_length) + np.random.randn(window_length))
        smoother = Smoother(smoother_name="left_gauss_linear")
        smoothed_signal1 = smoother.smooth(signal)
        smoother = Smoother(
            smoother_name="savgol", window_length=window_length, poly_fit_degree=1,
        )
        smoothed_signal2 = smoother.smooth(signal)

        assert np.allclose(smoothed_signal1, smoothed_signal2)

        window_length = 50
        signal = pd.Series(np.arange(window_length) + np.random.randn(window_length))
        smoother = Smoother(smoother_name="left_gauss_linear")
        smoothed_signal1 = signal.transform(smoother.smooth)
        smoother = Smoother(
            smoother_name="savgol", window_length=window_length, poly_fit_degree=1,
        )
        smoothed_signal2 = signal.transform(smoother.smooth)

        assert np.allclose(smoothed_signal1, smoothed_signal2)

        # The raw and smoothed lengths should match
        signal = pd.Series(np.ones(30))
        smoother = Smoother(smoother_name="moving_average")
        smoothed_signal = signal.transform(smoother.smooth)
        assert len(signal) == len(smoothed_signal)

        # The raw and smoothed arrays should be identical on constant data
        # modulo the nans
        signal = pd.Series(np.ones(30))
        window_length = 10
        smoother = Smoother(smoother_name="moving_average", window_length=window_length)
        smoothed_signal = signal.transform(smoother.smooth)
        assert np.allclose(
            signal[window_length - 1 :], smoothed_signal[window_length - 1 :]
        )
