from datetime import date, timedelta
from typing import Callable, List, Tuple

import numpy as np
from scipy.linalg import toeplitz
from scipy.sparse import diags as band

from ..data_containers import LocationSeries, SensorConfig
from ..sensorization.get_epidata import get_indicator_data


def _construct_convolution_matrix(signal: np.ndarray,
                                  kernel: np.ndarray) -> np.ndarray:
    """
    Constructs full convolution matrix (n+m-1) x n,
    where n is the signal length and m the kernel length.

    Parameters
    ----------
    signal
        array of values to convolve
    kernel
        array with convolution kernel values

    Returns
    -------
        convolution matrix
    """
    n = signal.shape[0]
    m = kernel.shape[0]
    padding = np.zeros(n - 1)
    first_col = np.r_[kernel, padding]
    first_row = np.r_[kernel[0], padding]

    return toeplitz(first_col, first_row)


def _fft_convolve(signal: np.ndarray, kernel: np.ndarray) -> np.ndarray:
    """
    Perform 1D convolution in the frequency domain.

    Parameters
    ----------
    signal
        array of values to convolve
    kernel
        array with convolution kernel values

    Returns
    -------
        array with convolved signal values

    """
    n = signal.shape[0]
    m = kernel.shape[0]
    signal_freq = np.fft.fft(signal, n + m - 1)
    kernel_freq = np.fft.fft(kernel, n + m - 1)
    return np.fft.ifft(signal_freq * kernel_freq).real[:n]


def deconvolve_tf(y: np.ndarray,
                  kernel: np.ndarray,
                  lam: float,
                  n_iters: int = 100,
                  k: int = 2,
                  clip: bool = False) -> np.ndarray:
    """
    Perform trend filtering regularized deconvolution through the following optimization

        minimize  (1/2n) ||y - Cx||_2^2 + lam*||D^(k+1)x||_1
            x

    where C is the discrete convolution matrix, and D^(k+1) the discrete differences
    operator. The second term adds a trend filtering (tf) penalty.

    Parameters
    ----------
    y
        array of values to convolve
    kernel
        array with convolution kernel values
    lam
        regularization parameter for trend filtering penalty smoothness
    n_iters
        number of ADMM interations to perform.
    k
        order of the trend filtering penalty.
    clip
        Boolean to clip count values to [0, infty).

    Returns
    -------
        array of the deconvolved signal values
    """

    def _soft_thresh(x: np.ndarray, lam: float) -> np.ndarray:
        """Perform soft-thresholding of x with threshold lam."""
        return np.sign(x) * np.maximum(np.abs(x) - lam, 0)

    n = y.shape[0]
    m = kernel.shape[0]
    rho = lam  # set equal
    C = _construct_convolution_matrix(y, kernel)[:n, ]
    D = band([-1, 1], [0, 1], shape=(n - 1, n)).toarray()
    D = np.diff(D, n=k, axis=0)

    # pre-calculations
    DtD = D.T @ D
    CtC = C.T @ C / n
    Cty = C.T @ y / n
    x_update_1 = np.linalg.inv(CtC + rho * DtD)

    # begin admm loop
    x_k = None
    alpha_0 = np.zeros(n - k - 1)
    u_0 = np.zeros(n - k - 1)
    for t in range(n_iters):
        x_k = x_update_1 @ (Cty + rho * D.T @ (alpha_0 - u_0))
        Dx_u0 = np.diff(x_k, n=(k + 1)) + u_0
        alpha_k = _soft_thresh(Dx_u0, lam / rho)
        u_k = Dx_u0 - alpha_k

        alpha_0 = alpha_k
        u_0 = u_k

    if clip:
        x_k = np.clip(x_k, 0, np.infty)

    return x_k


def deconvolve_tf_cv(y: np.ndarray,
                     kernel: np.ndarray,
                     method: str = "forward",
                     cv_grid: np.ndarray = np.logspace(1, 3.5, 10),
                     n_folds: int = 5,
                     n_iters: int = 100,
                     k: int = 2,
                     clip: bool = True,
                     verbose: bool = False) -> np.ndarray:
    """
    Run cross-validation to tune smoothness over deconvolve_tf(). Two types of CV are
    supported

        - "le3o", which leaves out every third value in training, and imputes the
          missing test value with the average of the neighboring points. The
          n_folds argument is ignored if method="le3o".
        - "forward", which trains on values 1,...,t and predicts the (t+1)th value as
          the fitted value for t. The n_folds argument decides the number of points
          to hold out, and then "walk forward".

    The smoothness parameter with the smallest mean-squared error is chosen.

    Parameters
    ----------
    y
        array of values to convolve
    kernel
        array with convolution kernel values
    method
        string with one of {"le3o", "forward"} specifying cv type
    cv_grid
        grid of trend filtering penalty values to search over
    n_folds
        number of splits for cv (see above documentation)
    n_iters
        number of ADMM interations to perform.
    k
        order of the trend filtering penalty.
    clip
        Boolean to clip count values to [0, infty)
    verbose
        Boolean whether to print debug statements

    Returns
    -------
        array of the deconvolved signal values
    """

    assert method in {"le3o", "forward"}, (
        "cv method specified should be one of {'le3o', 'forward'}"
    )

    n = y.shape[0]
    cv_loss = np.zeros((cv_grid.shape[0],))

    if method == "le3o":
        for i in range(3):
            if verbose: print(f"Fitting fold {i}/3")
            test_split = np.zeros((n,), dtype=bool)
            test_split[i::3] = True
            for j, reg_par in enumerate(cv_grid):
                x_hat = np.full((n,), np.nan)
                x_hat[~test_split] = deconvolve_tf(y[~test_split], kernel, reg_par,
                                                   n_iters, k, clip)
                x_hat = _impute_with_neighbors(x_hat)
                y_hat = _fft_convolve(x_hat, kernel)
                cv_loss[j] += np.sum((y[test_split] - y_hat[test_split]) ** 2)
    elif method == "forward":
        for i in range(1, n_folds + 1):
            if verbose: print(f"Fitting fold {i}/{n_folds}")
            for j, reg_par in enumerate(cv_grid):
                x_hat = np.full((n - i + 1,), np.nan)
                x_hat[:(n - i)] = deconvolve_tf(y[:(n - i)], kernel,
                                                reg_par, n_iters, k, clip)
                x_hat[-1] = x_hat[-2]
                y_hat = _fft_convolve(x_hat, kernel)
                cv_loss[j] += (y[-1] - y_hat[-1]) ** 2

    lam = cv_grid[np.argmin(cv_loss)]
    if verbose: print(f"Chosen parameter: {lam:.4}")
    x_hat = deconvolve_tf(y, kernel, lam, n_iters, k, clip)
    return x_hat


def _impute_with_neighbors(x: np.ndarray) -> np.ndarray:
    """
    Impute missing values with the average of the elements immediately
    before and after.

    Parameters
    ----------
    x
        signal with missing values

    Returns
    -------
        imputed signal
    """
    # handle edges
    if np.isnan(x[0]):
        x[0] = x[1]

    if np.isnan(x[-1]):
        x[-1] = x[-2]

    imputed_x = np.copy(x)
    for i, (a, b, c) in enumerate(zip(x, x[1:], x[2:])):
        if np.isnan(b):
            imputed_x[i + 1] = (a + c) / 2

    assert np.isnan(imputed_x).sum() == 0

    return imputed_x


def deconvolve_signal(convolved_truth_indicator: SensorConfig,
                      start_date: date,
                      end_date: date,
                      as_of_date: date,
                      input_locations: List[Tuple[str, str]],
                      kernel: np.ndarray,
                      fit_func: Callable = deconvolve_tf_cv,
                      ) -> List[LocationSeries]:
    """
    Compute ground truth signal value by deconvolving an indicator with a delay
    distribution.

    The deconvolution function is specified by fit_func, by default
    using a least-squares deconvolution with a trend filtering penalty, chosen
    by walk-forward validation.

    Parameters
    ----------
    convolved_truth_indicator
        (source, signal) tuple of quantity to deconvolve.
    start_date
        First date of data to use.
    end_date
        Last date of data to use.
    as_of_date
        Date that the data should be retrieved as of.
    input_locations
        List of (location, geo_type) tuples specifying locations to train and obtain nowcasts for.
    kernel
        Delay distribution from infection to report
    fit_func
        Fitting function for the deconvolution.

    Returns
    -------
        list of LocationSeries for the deconvolved signal
    """

    n_locs = len(input_locations)

    # def _to_date(date: Union[int, str], fmt: str = '%Y%m%d') -> date:
    #     """Convert int to date object, using specified fmt."""
    #     return datetime.strptime(str(date), fmt).date()

    # full date range
    n_full_dates = (end_date - start_date).days + 1
    full_dates = [start_date + timedelta(days=a) for a in range(n_full_dates)]
    # full_dates = [int(d.strftime('%Y%m%d')) for d in full_dates]

    # retrieve convolved signal
    combo_keys = []
    combo_series = []
    for loc, geo_type in input_locations:
        # output corresponds to order of input_locations
        combo_keys.append((convolved_truth_indicator.source,
                           convolved_truth_indicator.signal,
                           geo_type, loc))
        combo_series.append(LocationSeries(loc, geo_type))

    # epidata call to get convolved truth
    combo_convolved_truth = get_indicator_data([convolved_truth_indicator],
                                               combo_series,
                                               as_of_date)

    # perform deconvolution on each location individually
    deconvolved_truths = []
    for j, loc_key in enumerate(combo_keys):
        _, _, geo_type, loc = loc_key
        if loc_key in combo_convolved_truth:
            convolved_truth = combo_convolved_truth[loc_key]
            convolved_truth = convolved_truth.get_data_range(start_date, end_date)
            deconvolved_truth = fit_func(np.array(convolved_truth), kernel)
            deconvolved_truths.append(
                LocationSeries(loc,
                               geo_type,
                               dict(zip(full_dates, deconvolved_truth))))

        else:
            # return empty
            deconvolved_truths.append(LocationSeries(loc, geo_type))

        if (j + 1) % 25 == 0: print(f"Deconvolved {j}/{n_locs} locations")

    return deconvolved_truths
