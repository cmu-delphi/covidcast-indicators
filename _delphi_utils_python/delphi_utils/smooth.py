"""
This file contains the smoothing utility functions. We have a number of
possible smoothers to choose from: windowed average, local weighted regression,
and a causal Savitzky-Golay filter.

Code is courtesy of Dmitry Shemetov, Maria Jahja, and Addison Hu.

These smoothers are all functions that take a 1D numpy array and return a smoothed
1D numpy array of the same length (with a few np.nans in the beginning). See the
docstrings for details.
"""

import warnings

import numpy as np


class Smoother:
    """
    This is the smoothing utility class. This class holds the parameter settings for its smoother
    methods and provides reasonable defaults. Basic usage can be found in the examples below.

    The smoother function takes numpy arrays as input, expecting the values to come from a
    regularly-spaced time grid. NANs are ok, as long as the array does not begin with a NAN. The
    rest of the NANs will be handled via imputation by default, though this can be turned off.

    Parameters
    ----------
    smoother_name: {'savgol', 'left_gauss_linear', 'moving_average', 'identity'}
        This variable specifies the smoother. We have four smoothers, currently:
        * 'savgol' or a Savtizky-Golay smoother (default)
        * 'left_gauss_linear' or a Gaussian-weight linear regression smoother
        * 'moving_average' or a moving window average smoother
        * 'identity' or the trivial smoother (no smoothing)
        Descriptions of the smoothers are available in the doc strings. Full details are
        in: https://github.com/cmu-delphi/covidcast-modeling/indicators_smoother.
    window_length: int
        The length of the averaging window for 'savgol' and 'moving_average'.
        This value is in the units of the data, which tends to be days.
    gaussian_bandwidth: float or None
        If float, all regression is done with Gaussian weights whose variance is
        half the gaussian_bandwidth. If None, performs unweighted regression. (Applies
        to 'left_gauss_linear' and 'savgol'.)
        Here are some reference values:
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
        Currently only implemented for 'left_gauss_linear'.
    poly_fit_degree: int
        A parameter for the 'savgol' smoother which sets the degree of the polynomial fit.

    Methods
    ----------
    smooth: np.ndarray
        Takes a 1D signal and returns a smoothed version. Both arrays have the same length.

    Example Usage
    -------------
    Example 1. Apply a rolling average smoother with a window of length 10.
    >>> smoother = Smoother(smoother_name='moving_average', window_length=10)
    >>> smoothed_signal = smoother.smooth(signal)

    Example 2. Smooth a dataframe column.
    >>> smoother = Smoother(smoother_name="savgol")
    >>> df[col] = pd.Series(smoother.smooth(df[col].to_numpy()))

    Example 3. Apply a rolling weighted average smoother, with 95% weight on the recent 2 weeks and
               a sharp cutoff after 4 weeks.
    >>> smoother = Smoother(smoother_name='savgol', poly_fit_degree=0, window_length=28,
                          gaussian_bandwidth=144)
    >>> smoothed_signal = smoother.smooth(signal)

    Example 4. Apply a local linear regression smoother (equivalent to `left_gauss_linear`), with
               95% weight on the recent week and a sharp cutoff after 3 weeks.
    >>> smoother = Smoother(smoother_name='savgol', poly_fit_degree=1, window_length=21,
                          gaussian_bandwidth=36)
    >>> smoothed_signal = smoother.smooth(signal)

    Example 5. Apply the identity function (simplifies code that iterates through smoothers _and_
               raw data).
    >>> smoother = Smoother(smoother_name='identity')
    >>> smoothed_signal = smoother.smooth(signal)
    """

    def __init__(
        self,
        smoother_name="savgol",
        poly_fit_degree=2,
        window_length=28,
        gaussian_bandwidth=144,  # a ~2 week window
        impute_method="savgol",
        minval=None,
        boundary_method="shortened_window",
    ):
        self.smoother_name = smoother_name
        self.poly_fit_degree = poly_fit_degree
        self.window_length = window_length
        self.gaussian_bandwidth = gaussian_bandwidth
        self.impute_method = impute_method
        self.minval = minval
        self.boundary_method = boundary_method
        if smoother_name == "savgol":
            self.coeffs = self.savgol_coeffs(
                -self.window_length + 1,
                0,
                self.poly_fit_degree,
                self.gaussian_bandwidth,
            )
        else:
            self.coeffs = None

        valid_smoothers = {"savgol", "left_gauss_linear", "moving_average", "identity"}

        if self.smoother_name not in valid_smoothers:
            raise ValueError("Invalid smoother name given.")

        valid_impute_methods = {"savgol", "zeros", None}

        if self.impute_method not in valid_impute_methods:
            raise ValueError("Invalid impute method given.")

    def smooth(self, signal):
        """
        The major workhorse smoothing function. Can use one of three smoothing
        methods, as specified by the class variable smoother_name.

        Parameters
        ----------
        signal: np.ndarray
            A 1D signal to be smoothed.

        signal_smoothed: np.ndarray
            A smoothed 1D signal.
        """
        if len(signal) < self.window_length:
            raise ValueError("The window_length must be smaller than the length of the signal.")

        signal = self.impute(signal)

        if self.smoother_name == "savgol":
            signal_smoothed = self.savgol_smoother(signal)
        elif self.smoother_name == "left_gauss_linear":
            signal_smoothed = self.left_gauss_linear_smoother(signal)
        elif self.smoother_name == "moving_average":
            signal_smoothed = self.moving_average_smoother(signal)
        elif self.smoother_name == "identity":
            signal_smoothed = signal

        return signal_smoothed

    def impute(self, signal):
        """
        Imputes the nan values in the signal.

        Parameters
        ----------
        signal: np.ndarray
            1D signal to be imputed.

        Returns
        -------
        imputed_signal: np.ndarray
            Imputed signal.
        """
        if self.impute_method == "savgol":
            imputed_signal = self.savgol_impute(signal)
        elif self.impute_method == "zeros":
            imputed_signal = np.nan_to_num(signal)
        elif self.impute_method is None:
            imputed_signal = np.copy(signal)

        return imputed_signal

    def moving_average_smoother(self, signal):
        """
        Computes a moving average on the signal.

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
        """
        Smooth the y-values using a local linear regression with Gaussian weights.

        At each time t, we use the data from times 1, ..., t-dt, weighted
        using the Gaussian kernel, to produce the estimate at time t.

        For math details, see the smoothing_docs folder.

        Parameters
        ----------
        signal: np.ndarray
            A 1D signal.

        Returns
        ----------
        signal_smoothed: np.ndarray
            A smoothed 1D signal.
        """
        n = len(signal)
        signal_smoothed = np.zeros_like(signal)
        A = np.vstack([np.ones(n), np.arange(n)]).T  # the regression design matrix
        for idx in range(n):
            weights = np.exp(
                -((np.arange(idx + 1) - idx) ** 2) / self.gaussian_bandwidth
            )
            AwA = np.dot(A[: (idx + 1), :].T * weights, A[: (idx + 1), :])
            Awy = np.dot(
                A[: (idx + 1), :].T * weights, signal[: (idx + 1)].reshape(-1, 1)
            )
            try:
                beta = np.linalg.solve(AwA, Awy)
                signal_smoothed[idx] = np.dot(A[: (idx + 1), :], beta)[-1]
            except np.linalg.LinAlgError:
                signal_smoothed[idx] = signal[idx] if self.impute else np.nan
        if self.minval is not None:
            signal_smoothed[signal_smoothed <= self.minval] = self.minval
        return signal_smoothed

    def savgol_predict(self, signal):
        """
        Fits a polynomial through the values given by the signal and returns the value
        of the polynomial at the right-most signal-value. More precisely, fits a polynomial
        f(t) of degree poly_fit_degree through the points signal[0], signal[1] ..., signal[-1],
        and returns the evaluation of the polynomial at the location of signal[-1].

        Parameters
        ----------
        signal: np.ndarray
            A 1D signal to smooth.

        Returns
        ----------
        coeffs: np.ndarray
            A vector of coefficients of length nl that determines the savgol
            convolution filter.
        """
        coeffs = self.savgol_coeffs(
            -len(signal) + 1, 0, self.poly_fit_degree, self.gaussian_bandwidth
        )
        return signal @ coeffs

    @classmethod
    def savgol_coeffs(cls, nl, nr, poly_fit_degree, gaussian_bandwidth=100):
        """
        Solves for the Savitzky-Golay coefficients. The coefficients c_i
        give a filter so that
            y = \sum_{i=-{n_l}}^{n_r} c_i x_i
        is the value at 0 (thus the constant term) of the polynomial fit
        through the points {x_i}. The coefficients are c_i are caluclated as
            c_i =  ((A.T @ A)^(-1) @ (A.T @ e_i))_0
        where A is the design matrix of the polynomial fit and e_i is the standard
        basis vector i. This is currently done via a full inversion, which can be
        optimized.

        Parameters
        ----------
        nl: int
            The left window bound for the polynomial fit.
        nr: int
            The right window bound for the polynomial fit.
        poly_fit_degree: int
            The degree of the polynomial to be fit.
        gaussian_bandwidth: float or None
            If float, performs regression with Gaussian weights whose variance is
            the gaussian_bandwidth. If None, performs unweighted regression.

        Returns
        ----------
        coeffs: np.ndarray
            A vector of coefficients of length nl that determines the savgol
            convolution filter.
        """
        if nl >= nr:
            raise ValueError("The left window bound should be less than the right.")
        if nr > 0:
            raise warnings.warn("The filter is no longer causal.")

        A = np.vstack(
            [np.arange(nl, nr + 1) ** j for j in range(poly_fit_degree + 1)]
        ).T

        if gaussian_bandwidth is None:
            mat_inverse = np.linalg.inv(A.T @ A) @ A.T
        else:
            weights = np.exp(-((np.arange(nl, nr + 1)) ** 2) / gaussian_bandwidth)
            mat_inverse = np.linalg.inv((A.T * weights) @ A) @ (A.T * weights)
        window_length = nr - nl + 1
        basis_vector = np.zeros(window_length)
        coeffs = np.zeros(window_length)
        for i in range(window_length):
            basis_vector = np.zeros(window_length)
            basis_vector[i] = 1.0
            coeffs[i] = (mat_inverse @ basis_vector)[0]
        return coeffs

    def savgol_smoother(self, signal):
        """
        Returns a specific type of convolution of the 1D signal with the 1D signal
        coeffs, respecting boundary effects. That is, the output y is
            signal_smoothed_i = signal_i
            signal_smoothed_i = sum_{j=0}^n coeffs_j signal_{i+j}, if i >= len(coeffs) - 1
        In words, entries close to the left boundary are not smoothed, the window does
        not proceed over the right boundary, and the rest of the values are regular
        convolution.

        Parameters
        ----------
        signal: np.ndarray
            A 1D signal.

        Returns
        ----------
        signal_smoothed: np.ndarray
            A smoothed 1D signal of same length as signal.
        """

        # reverse because np.convolve reverses the second argument
        temp_reversed_coeffs = np.array(list(reversed(self.coeffs)))

        # does the majority of the smoothing, with the calculated coefficients
        signal_padded = np.append(np.nan * np.ones(len(self.coeffs) - 1), signal)
        signal_smoothed = np.convolve(signal_padded, temp_reversed_coeffs, mode="valid")

        # this section handles the smoothing behavior at the (left) boundary:
        # - shortened_window (default) applies savgol with a smaller window to do the fit
        # - identity keeps the original signal (doesn't smooth)
        # - nan writes nans
        if self.boundary_method == "shortened_window":
            for ix in range(len(self.coeffs)):
                if ix == 0:
                    signal_smoothed[ix] = signal[ix]
                else:
                    try:
                        signal_smoothed[ix] = self.savgol_predict(signal[: (ix + 1)])
                    except np.linalg.LinAlgError:  # for small ix, the design matrix is singular
                        signal_smoothed[ix] = signal[ix]
            return signal_smoothed
        elif self.boundary_method == "identity":
            for ix in range(len(self.coeffs)):
                signal_smoothed[ix] = signal[ix]
            return signal_smoothed
        elif self.boundary_method == "nan":
            return signal_smoothed
        else:
            raise ValueError("Unknown boundary method.")

    def savgol_impute(self, signal):
        """
        This method fills the nan values in the signal with an M-degree polynomial fit
        on a rolling window of the immediate past up to window_length data points.

        In the case of a single data point in the past, the single data point is
        continued. In the case of no data points in the past (i.e. the signal starts
        with nan), an error is raised.

        Note that in the case of many adjacent nans, the method will use previously
        imputed values to do the fitting for later values. E.g. for
        >>> x = np.array([1.0, 2.0, np.nan, 1.0, np.nan])
        the last np.nan will be fit on np.array([1.0, 2.0, *, 1.0]), where * is the
        result of imputing based on np.array([1.0, 2.0]) (depends on the savgol
        settings).

        Parameters
        ----------
        signal: np.ndarray
            A 1D signal to be imputed.

        Returns
        ----------
        signal_imputed: np.ndarray
            An imputed 1D signal.
        """
        signal_imputed = np.copy(signal)
        if np.isnan(signal[0]):
            raise ValueError("The signal should not begin with a nan value.")
        for ix in np.where(np.isnan(signal))[0]:
            if ix < self.window_length:
                if ix == 1:
                    signal_imputed[ix] = signal_imputed[0]
                else:
                    coeffs = self.savgol_coeffs(
                        -ix, -1, self.poly_fit_degree, self.gaussian_bandwidth
                    )
                    signal_imputed[ix] = signal_imputed[:ix] @ coeffs
            else:
                coeffs = self.savgol_coeffs(
                    -self.window_length,
                    -1,
                    self.poly_fit_degree,
                    self.gaussian_bandwidth,
                )
                signal_imputed[ix] = signal_imputed[ix - self.window_length : ix] @ coeffs
        return signal_imputed
