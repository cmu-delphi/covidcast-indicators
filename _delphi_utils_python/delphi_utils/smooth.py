"""Smoother utility.

This file contains the smoothing utility functions. We have a number of
possible smoothers to choose from: windowed average, local weighted regression,
and a causal Savitzky-Golay filter.

Code is courtesy of Dmitry Shemetov, Maria Jahja, and Addison Hu.

These smoothers are all functions that take a 1D numpy array and return a smoothed
1D numpy array of the same length (with a few np.nans in the beginning). See the
docstrings for details.
"""

from typing import Union
import warnings

import numpy as np
import pandas as pd


class Smoother:  # pylint: disable=too-many-instance-attributes
    """Smoother class.

    This is the smoothing utility class. This class holds the parameter settings for its smoother
    methods and provides reasonable defaults. Basic usage can be found in the examples below.

    The smoother function takes numpy arrays or pandas Series as input, expecting the values to be
    on a regularly-spaced time grid. NANs are ok, as long as the array does not begin with a NAN.
    The rest of the NANs will be handled via imputation by default, though this can be turned off.

    Parameters
    ----------
    smoother_name: {'savgol', 'moving_average', 'identity', 'left_gauss_linear'}
        This variable specifies the smoother. We have four smoothers, currently:
        * 'savgol' or a Savtizky-Golay smoother (default)
        * 'moving_average' or a moving window average smoother
        * 'identity' or the trivial smoother (no smoothing)
        * 'left_gauss_linear' or a Gaussian-weight linear regression smoother
        Descriptions of the smoothers are available in the doc strings. Full mathematical
        details are in: https://github.com/cmu-delphi/covidcast-modeling/ in the folder
        'indicator_smoother'.
    poly_fit_degree: int
        A parameter for the 'savgol' smoother which sets the degree of the polynomial fit.
    window_length: int
        The length of the fitting window for 'savgol' and the averaging window 'moving_average'.
        This value is in the units provided by the data, which are likely to be days for Delphi.
        Note that if window_length is smaller than the length of the signal, then only the
        imputation method is run on the signal.
    gaussian_bandwidth: float or None
        If float, all regression is done with Gaussian weights whose variance is
        half the gaussian_bandwidth. If None, performs unweighted regression. (Applies
        to 'left_gauss_linear' and 'savgol'.)
        Here are some reference values (the given bandwidth produces a 95% weighting on
        the data of length time window into the past):
        time window    |     bandwidth
        7                        36
        14                       144
        21                       325
        28                       579
        35                       905
        42                       1303
    impute: {'savgol', 'zeros', None}
        If 'savgol' (default), will fill nan values with a savgol fit on the largest available time
        window prior (up to window_length). If 'zeros', will fill nan values with zeros.
        If None, leaves the nans in place.
    minval: float or None
        The smallest value to allow in a signal. If None, there is no smallest value.
        Currently only implemented for 'left_gauss_linear'. This should probably not be in the scope
        of the smoothing utility.
    boundary_method: {'shortened_window', 'identity', 'nan'}
        Determines how the 'savgol' method handles smoothing at the (left) boundary, where the past
        data length is shorter than the window_length parameter. If 'shortened_window', it uses the
        maximum window available; at the very edge (generally up to poly_fit_degree) it keeps the
        same value as the raw signal. If 'identity', it just keeps the raw signal. If 'nan', it
        writes nans. For the other smoothing methods, 'moving_average' writes nans and
        'left_gauss_linear' uses a shortened window.

    Methods
    ----------
    smooth: np.ndarray or pd.Series
        Takes a 1D signal and returns a smoothed version.
        The input and the output have the same length and type.

    Example Usage
    -------------
    Example 1. Apply a rolling average smoother with a window of length 10.
    >>> smoother = Smoother(smoother_name='moving_average', window_length=10)
    >>> smoothed_signal = smoother.smooth(signal)

    Example 2. Smooth a dataframe column.
    >>> smoother = Smoother(smoother_name='savgol')
    >>> df[col] = df[col].transform(smoother.smooth)

    Example 3. Apply a rolling weighted average smoother, with 95% weight on the recent 2 weeks and
               a sharp cutoff after 4 weeks.
    >>> smoother = Smoother(smoother_name='savgol', poly_fit_degree=0, window_length=28,
                          gaussian_bandwidth=144)
    >>> smoothed_signal = smoother.smooth(signal)

    Example 4. Apply a local linear regression smoother (essentially equivalent to
               `left_gauss_linear`), with 95% weight on the recent week and a sharp
               cutoff after 3 weeks.
    >>> smoother = Smoother(smoother_name='savgol', poly_fit_degree=1, window_length=21,
                          gaussian_bandwidth=36)
    >>> smoothed_signal = smoother.smooth(signal)

    Example 5. Apply the identity function (simplifies code that iterates through smoothers _and_
               expects a copy of the raw data).
    >>> smoother = Smoother(smoother_name='identity')
    >>> smoothed_signal = smoother.smooth(signal)
    """

    def __init__(
        self,
        smoother_name="savgol",
        poly_fit_degree=2,
        window_length=28,
        gaussian_bandwidth=144,  # a ~2 week window
        impute_method=None,
        minval=None,
        boundary_method="shortened_window",
    ):
        """See class docstring."""
        self.smoother_name = smoother_name
        self.poly_fit_degree = poly_fit_degree
        self.window_length = window_length
        self.gaussian_bandwidth = gaussian_bandwidth
        self.impute_method = self._select_imputer(impute_method, self.smoother_name)
        self.minval = minval
        self.boundary_method = boundary_method

        valid_smoothers = {"savgol", "left_gauss_linear", "moving_average", "identity"}
        valid_impute_methods = {"savgol", "zeros", "identity"}
        valid_boundary_methods = {"shortened_window", "identity", "nan"}
        if self.smoother_name not in valid_smoothers:
            raise ValueError("Invalid smoother_name given.")
        if self.impute_method not in valid_impute_methods:
            raise ValueError("Invalid impute_method given.")
        if self.boundary_method not in valid_boundary_methods:
            raise ValueError("Invalid boundary_method given.")
        if self.window_length <= 1:
            raise ValueError("Window length is too short.")

        if smoother_name == "savgol":
            # The polynomial fitting is done on a past window of size window_length
            # including the current day value.
            self.coeffs = self.savgol_coeffs(
                -self.window_length + 1, 0, self.poly_fit_degree
            )
        else:
            self.coeffs = None

    def _select_imputer(self, impute_method, smoother_name):
        if impute_method is None and smoother_name != "identity":
            return "savgol"
        if impute_method is None and smoother_name == "identity":
            return "identity"
        return impute_method

    def smooth(
        self, signal: Union[np.ndarray, pd.Series], impute_order=2
    ) -> Union[np.ndarray, pd.Series]:
        """Apply a smoother to a signal.

        The major workhorse smoothing function. Imputes the nans and then applies
        a smoother to the signal.

        Parameters
        ----------
        signal: np.ndarray or pd.Series
            A 1D signal to be smoothed.
        impute_order: int
            The polynomial order of the fit used for imputation. By default, this is set to
            2.

        Returns
        ----------
        signal_smoothed: np.ndarray or pd.Series
            A smoothed 1D signal. Returns an array of the same type and length as
            the input.
        """
        # If all nans, pass through
        if np.all(np.isnan(signal)):
            return signal

        is_pandas_series = isinstance(signal, pd.Series)
        pandas_index = signal.index if is_pandas_series else None
        signal = signal.to_numpy() if is_pandas_series else signal

        # Find where the first non-nan value is located and truncate the initial nans
        ix = np.where(~np.isnan(signal))[0][0]
        signal = signal[ix:]

        # Don't smooth in certain edge cases
        if len(signal) < self.poly_fit_degree or len(signal) == 1:
            signal_smoothed = signal.copy()
        else:
            # Impute
            signal = self.impute(signal, impute_order=impute_order)

            # Smooth
            signal_smoothed = self._select_smoother()(signal)

        # Append the nans back, since we want to preserve length
        signal_smoothed = np.hstack([np.nan * np.ones(ix), signal_smoothed])
        # Convert back to pandas if necessary
        if is_pandas_series:
            signal_smoothed = pd.Series(signal_smoothed)
            signal_smoothed.index = pandas_index
        return signal_smoothed

    def _select_smoother(self):
        """Select a smoothing method based on the smoother type."""
        if self.smoother_name == "savgol":
            return self.savgol_smoother
        if self.smoother_name == "left_gauss_linear":
            return self.left_gauss_linear_smoother
        if self.smoother_name == "moving_average":
            return self.moving_average_smoother
        if self.smoother_name == "identity":
            return lambda x: x
        raise ValueError(f"invalid smoother {self.smoother_name}")

    def impute(self, signal, impute_order=2):
        """Impute the nan values in the signal.

        See the class docstring for an explanation of the impute methods.

        Parameters
        ----------
        signal: np.ndarray
            1D signal to be imputed.
        impute_order: int
            The polynomial order of the fit used for imputation.

        Returns
        -------
        imputed_signal: np.ndarray
            Imputed signal.
        """
        if self.impute_method == "savgol":
            # We cannot impute if the signal begins with a NaN (there is no information to go by).
            # To preserve input-output array lengths, this util will not drop NaNs for you.
            if np.isnan(signal[0]):
                raise ValueError("The signal should not begin with a nan value.")
            imputed_signal = self.savgol_impute(signal, impute_order)
        elif self.impute_method == "zeros":
            imputed_signal = np.nan_to_num(signal)
        elif self.impute_method == "identity":
            imputed_signal = np.copy(signal)

        return imputed_signal

    def moving_average_smoother(self, signal):
        """Compute a moving average on the signal.

        Parameters
        ----------
        signal: np.ndarray
            Input array.

        Returns
        -------
        signal_smoothed: np.ndarray
            An array with the same length as arr, but the first window_length-1
            entries are np.nan.
        """
        if not isinstance(self.window_length, int):
            raise ValueError("k must be int.")

        signal_padded = np.append(np.nan * np.ones(self.window_length - 1), signal)
        signal_smoothed = (
            np.convolve(
                signal_padded, np.ones(self.window_length, dtype=int), mode="valid"
            )
            / self.window_length
        )

        return signal_smoothed

    def left_gauss_linear_smoother(self, signal):
        """Smooth the y-values using a local linear regression with Gaussian weights.

        DEPRECATED: This method is available to help sanity check the 'savgol' method.
        Use 'savgol' with poly_fit_degree=1 and the appropriate gaussian_bandwidth instead.

        At each time t, we use the data from times 1, ..., t-dt, weighted
        using the Gaussian kernel, to produce the estimate at time t.

        Parameters
        ----------
        signal: np.ndarray
            A 1D signal.

        Returns
        ----------
        signal_smoothed: np.ndarray
            A smoothed 1D signal.
        """
        warnings.warn(
            "Use the savgol smoother with poly_fit_degree=1 instead.",
            DeprecationWarning,
        )
        n = len(signal)
        signal_smoothed = np.zeros_like(signal)
        # A is the regression design matrix
        A = np.vstack([np.ones(n), np.arange(n)]).T  # pylint: disable=invalid-name
        for idx in range(n):
            weights = np.exp(
                -((np.arange(idx + 1) - idx) ** 2) / self.gaussian_bandwidth
            )
            AwA = np.dot(  # pylint: disable=invalid-name
                A[: (idx + 1), :].T * weights, A[: (idx + 1), :]
            )
            Awy = np.dot(  # pylint: disable=invalid-name
                A[: (idx + 1), :].T * weights, signal[: (idx + 1)].reshape(-1, 1)
            )
            try:
                beta = np.linalg.solve(AwA, Awy)
                signal_smoothed[idx] = np.dot(A[: (idx + 1), :], beta)[-1]
            except np.linalg.LinAlgError:
                signal_smoothed[idx] = (
                    signal[idx]  # pylint: disable=using-constant-test
                    if self.impute
                    else np.nan
                )
        if self.minval is not None:
            signal_smoothed[signal_smoothed <= self.minval] = self.minval
        return signal_smoothed

    def savgol_predict(self, signal, poly_fit_degree, nr):
        """Predict a single value using the savgol method.

        Fits a polynomial through the values given by the signal and returns the value
        of the polynomial at the right-most signal-value. More precisely, for a signal of length
        n, fits a poly_fit_degree polynomial through the points signal[-n+1+nr], signal[-n+2+nr],
        ..., signal[nr], and returns the evaluation of the polynomial at signal[0]. Hence, if
        nr=0, then the last value of the signal is smoothed, and if nr=-1, then the value after
        the last signal value is anticipated.

        Parameters
        ----------
        signal: np.ndarray
            A 1D signal to smooth.
        poly_fit_degree: int
            The degree of the polynomial fit.
        nr: int
            An integer that determines the position of the predicted value relative to the signal.

        Returns
        ----------
        predicted_value: float
            The anticipated value that comes after the end of the signal based on a polynomial fit.
        """
        coeffs = self.savgol_coeffs(-len(signal) + 1 + nr, nr, poly_fit_degree)
        predicted_value = signal @ coeffs
        return predicted_value

    def savgol_coeffs(self, nl, nr, poly_fit_degree):
        """Solve for the Savitzky-Golay coefficients.

        Solves for the Savitzky-Golay coefficients. The coefficients c_i
        give a filter so that
            y = sum_{i=-{n_l}}^{n_r} c_i x_i
        is the value at 0 (thus the constant term) of the polynomial fit
        through the points {x_i}. The coefficients are c_i are calculated as
            c_i =  ((A.T @ A)^(-1) @ (A.T @ e_i))_0
        where A is the design matrix of the polynomial fit and e_i is the standard
        basis vector i. This is currently done via a full inversion, which can be
        optimized.

        Parameters
        ----------
        nl: int
            The left window bound for the polynomial fit, inclusive.
        nr: int
            The right window bound for the polynomial fit, inclusive.
        poly_fit_degree: int
            The degree of the polynomial to be fit.

        Returns
        ----------
        coeffs: np.ndarray
            A vector of coefficients of length nr - nl + 1 that determines the savgol
            convolution filter.
        """
        if nl >= nr:
            raise ValueError("The left window bound should be less than the right.")
        if nr > 0:
            warnings.warn("The filter is no longer causal.")

        A = np.vstack(  # pylint: disable=invalid-name
            [np.arange(nl, nr + 1) ** j for j in range(poly_fit_degree + 1)]
        ).T

        if self.gaussian_bandwidth is None:
            mat_inverse = np.linalg.inv(A.T @ A) @ A.T
        else:
            weights = np.exp(-((np.arange(nl, nr + 1)) ** 2) / self.gaussian_bandwidth)
            mat_inverse = np.linalg.inv((A.T * weights) @ A) @ (A.T * weights)
        window_length = nr - nl + 1
        coeffs = np.zeros(window_length)
        for i in range(window_length):
            basis_vector = np.zeros(window_length)
            basis_vector[i] = 1.0
            coeffs[i] = (mat_inverse @ basis_vector)[0]
        return coeffs

    def savgol_smoother(self, signal):  # pylint: disable=inconsistent-return-statements
        """Smooth signal with the savgol smoother.

        Returns a convolution of the 1D signal with the Savitzky-Golay coefficients, respecting
        boundary effects. For an explanation of boundary effects methods, see the class docstring.

        Parameters
        ----------
        signal: np.ndarray
            A 1D signal.

        Returns
        ----------
        signal_smoothed: np.ndarray
            A smoothed 1D signal of same length as signal.
        """
        # Reverse because np.convolve reverses the second argument
        temp_reversed_coeffs = np.array(list(reversed(self.coeffs)))

        # Smooth the part of the signal away from the boundary first
        signal_padded = np.append(np.nan * np.ones(len(self.coeffs) - 1), signal)
        signal_smoothed = np.convolve(signal_padded, temp_reversed_coeffs, mode="valid")

        # This section handles the smoothing behavior at the (left) boundary:
        # - shortened_window (default) applies savgol with a smaller window to do the fit
        # - identity keeps the original signal (doesn't smooth)
        # - nan writes nans
        if self.boundary_method == "nan":
            return signal_smoothed

        # boundary methods "identity" and "shortened window"
        for ix in range(min(len(self.coeffs), len(signal))):
            if ix == 0 or self.boundary_method == "identity":
                signal_smoothed[ix] = signal[ix]
            else:
                # At the very edge, the design matrix is often singular, in which case
                # we just fall back to the raw signal
                try:
                    signal_smoothed[ix] = self.savgol_predict(
                        signal[: ix + 1], self.poly_fit_degree, 0
                    )
                except np.linalg.LinAlgError:  # for small ix, the design matrix is singular
                    signal_smoothed[ix] = signal[ix]
        return signal_smoothed

    def savgol_impute(self, signal, impute_order):
        """Impute the nan values in signal using savgol.

        This method fills the nan values in the signal with polynomial interpolation
        on a rolling window of the immediate past up to window_length data points.

        A number of boundary cases are handled involving nan filling close to the boundary.

        Note that in the case of many adjacent nans, the method will use previously
        imputed values to do the fitting for later values.

        Parameters
        ----------
        signal: np.ndarray
            A 1D signal to be imputed.
        impute_order: int
            The polynomial order of the fit used for imputation.

        Returns
        ----------
        signal_imputed: np.ndarray
            An imputed 1D signal.
        """
        if impute_order > self.window_length:
            raise ValueError("Impute order must be smaller than window length.")

        signal_imputed = np.copy(signal)
        for ix in np.where(np.isnan(signal_imputed))[0]:
            # Boundary cases
            if ix < self.window_length:
                # At the boundary, a single value should just be extended
                if ix == 1:
                    signal_imputed[ix] = signal_imputed[ix - 1]
                # Otherwise, use savgol fitting on the largest window prior,
                # reduce the polynomial degree if needed (can't fit if the
                # imputation order is larger than the available data)
                else:
                    signal_imputed[ix] = self.savgol_predict(
                        signal_imputed[:ix], min(ix - 1, impute_order), -1
                    )
            # Away from the boundary, use savgol fitting on a fixed window
            else:
                signal_imputed[ix] = self.savgol_predict(
                    signal_imputed[ix - self.window_length : ix],
                    impute_order,
                    -1,
                )
        return signal_imputed
