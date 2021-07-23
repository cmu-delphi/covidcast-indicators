from datetime import date, timedelta
from functools import partial
from typing import Callable, List, Tuple

import numpy as np
from scipy.linalg import toeplitz
from scipy.sparse import diags as band

from ..data_containers import LocationSeries, SensorConfig
from ..epidata import get_indicator_data


def _construct_convolution_matrix(signal: np.ndarray,
                                  kernel: np.ndarray,
                                  norm: bool) -> np.ndarray:
    """
    Constructs full convolution matrix (n+m-1) x n,
    where n is the signal length and m the kernel length.

    Parameters
    ----------
    signal
        array of values to convolve
    kernel
        array with convolution kernel values
    norm
        boolean whether to normalize rows to sum to sum(kernel)

    Returns
    -------
        convolution matrix
    """
    n = signal.shape[0]
    m = kernel.shape[0]
    padding = np.zeros(n - 1)
    first_col = np.r_[kernel, padding]
    first_row = np.r_[kernel[0], padding]

    P = toeplitz(first_col, first_row)

    if norm:
        scale = P.sum(axis=1) / kernel.sum()
        return P / scale[:, np.newaxis]
    return P


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


def _soft_thresh(x: np.ndarray, lam: float) -> np.ndarray:
    """Perform soft-thresholding of x with threshold lam."""
    return np.sign(x) * np.maximum(np.abs(x) - lam, 0)


def deconvolve_tf(y: np.ndarray,
                  x: np.ndarray,
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
    x
        array of positions
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

    n = y.shape[0]
    m = kernel.shape[0]
    rho = lam  # set equal
    C = _construct_convolution_matrix(y, kernel, False)[:n, ]
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


def _construct_poly_interp_mat(x, n, k):
    assert k == 3, "poly interpolation matrix only constructed for k=3"
    S = np.zeros((n, n - k - 1))
    S[0, 0] = (x[3] - x[0]) / (x[3] - x[2])
    S[0, 1] = (x[0] - x[2]) / (x[3] - x[2])
    S[1, 0] = (x[3] - x[1]) / (x[3] - x[2])
    S[1, 1] = (x[1] - x[2]) / (x[3] - x[2])
    S[n - 2, n - 6] = (x[n - 3] - x[n - 2]) / (x[n - 3] - x[n - 4])
    S[n - 2, n - 5] = (x[n - 2] - x[n - 4]) / (x[n - 3] - x[n - 4])
    S[n - 1, n - 6] = (x[n - 3] - x[n - 1]) / (x[n - 3] - x[n - 4])
    S[n - 1, n - 5] = (x[n - 1] - x[n - 4]) / (x[n - 3] - x[n - 4])
    S[2:(n - 2), :] = np.eye(n - k - 1)
    return S


def deconvolve_ntf(y: np.ndarray,
                   x: np.ndarray,
                   kernel: np.ndarray,
                   lam: float,
                   n_iters: int = 200,
                   k: int = 3,
                   clip: bool = False) -> np.ndarray:
    """
    Perform natural trend filtering regularized deconvolution. Only implemented for k=3.

    Parameters
    ----------
    y
        array of values to convolve
    x
        array of positions
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
    assert k == 3, "Natural TF only implemented for k=3"

    k = 3
    n = y.shape[0]
    m = kernel.shape[0]
    rho = lam  # set equal
    C = _construct_convolution_matrix(y, kernel, False)[:n, ]
    P = _construct_poly_interp_mat(x, n, k)
    D = band([-1, 1], [0, 1], shape=(n - 1, n)).toarray()
    D = np.diff(D, n=k, axis=0)
    C = C @ P
    D = D @ P

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
        Dx_u0 = D @ x_k + u_0
        alpha_k = _soft_thresh(Dx_u0, lam / rho)
        u_k = Dx_u0 - alpha_k

        alpha_0 = alpha_k
        u_0 = u_k

    x_k = P @ x_k
    if clip:
        x_k = np.clip(x_k, 0, np.infty)

    return x_k


def deconvolve_double_smooth_ntf(
        y: np.ndarray,
        x: np.ndarray,
        kernel: np.ndarray,
        lam: float,
        gam: float,
        n_iters: int = 1000,
        k: int = 3,
        clip: bool = False) -> np.ndarray:
    """
        Perform natural trend filtering regularized deconvolution. Only implemented for k=3.

        Parameters
        ----------
        y
            array of values to convolve
        x
            array of positions
        kernel
            array with convolution kernel values
        lam
            regularization parameter for trend filtering penalty smoothness
        gam
            regularization parameter for penalty on first differences of boundary points
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
    assert k == 3, "Natural TF only implemented for k=3"
    n = y.shape[0]
    m = kernel.shape[0]
    rho = lam  # set equal
    C = _construct_convolution_matrix(y, kernel, False)[:n, ]
    D = band([-1, 1], [0, 1], shape=(n - 1, n)).toarray()
    D = np.diff(D, n=k, axis=0)
    P = _construct_poly_interp_mat(x, n, k)
    D_m = band([-1, 1], [0, 1], shape=(n - 1, n)).toarray()
    D_m[:-m, :] = 0

    # kernel weights for double smoothing
    weights = np.ones((D_m.shape[0],))
    weights[-m:] = np.cumsum(kernel[::-1])
    weights /= np.max(weights)
    D_m = np.sqrt(np.diag(2 * gam * weights)) @ D_m
    C = C @ P
    D = D @ P
    D_m = D_m @ P

    # pre-calculations
    DtD = D.T @ D
    DmtDm = D_m.T @ D_m
    CtC = C.T @ C / n
    Cty = C.T @ y / n
    x_update_1 = np.linalg.inv(DmtDm + CtC + rho * DtD)

    # begin admm loop
    x_k = None
    alpha_0 = np.zeros(n - k - 1)
    u_0 = np.zeros(n - k - 1)
    for t in range(n_iters):
        x_k = x_update_1 @ (Cty + rho * D.T @ (alpha_0 + u_0))
        Dx = D @ x_k
        alpha_k = _soft_thresh(Dx - u_0, lam / rho)
        u_k = u_0 + alpha_k - Dx

        alpha_0 = alpha_k
        u_0 = u_k

    x_k = P @ x_k
    if clip:
        x_k = np.clip(x_k, 0, np.infty)
    return x_k


def deconvolve_tf_cv(y: np.ndarray,
                     x: np.ndarray,
                     kernel: np.ndarray,
                     fit_func: Callable = deconvolve_tf,
                     cv_method: str = "forward",
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
    x
        array of positions
    kernel
        array with convolution kernel values
    fit_func
        deconvolution function to use
    cv_method
        string with one of {"le3o", "forward"} specifying cv type
    cv_grid
        grid of trend filtering penalty values to search over
    n_folds
        number of splits for cv (see above documentation)
    n_iters
        number of ADMM interations to perform.
    k
        order of the trend filtering penalty.
    natural
        Boolean whether to use natural trend filtering. If True, k is fixed to 3.
    clip
        Boolean to clip count values to [0, infty)
    verbose
        Boolean whether to print debug statements


    Returns
    -------
        array of the deconvolved signal values
    """

    assert cv_method in {"le3o", "forward"}, (
        "cv method specified should be one of {'le3o', 'forward'}"
    )

    fit_func = partial(fit_func, kernel=kernel, n_iters=n_iters, k=k, clip=clip)
    n = y.shape[0]
    cv_loss = np.zeros((cv_grid.shape[0],))

    if cv_method == "le3o":
        for i in range(3):
            if verbose: print(f"Fitting fold {i}/3")
            test_split = np.zeros((n,), dtype=bool)
            test_split[i::3] = True
            for j, reg_par in enumerate(cv_grid):
                x_hat = np.full((n,), np.nan)
                x_hat[~test_split] = fit_func(y=y[~test_split], x=x[~test_split],
                                              lam=reg_par)
                x_hat = _impute_with_neighbors(x_hat)
                y_hat = _fft_convolve(x_hat, kernel)
                cv_loss[j] += np.sum((y[test_split] - y_hat[test_split]) ** 2)
    elif cv_method == "forward":
        def _linear_extrapolate(x0, y0, x1, y1, x_new):
            return y0 + ((x_new - x0) / (x1 - x0)) * (y1 - y0)

        for i in range(1, n_folds + 1):
            if verbose: print(f"Fitting fold {i}/{n_folds}")
            for j, reg_par in enumerate(cv_grid):
                x_hat = np.full((n - i + 1,), np.nan)
                x_hat[:(n - i)] = fit_func(y=y[:(n - i)], x=x[:(n - i)], lam=reg_par)
                # x_hat[-1] = x_hat[-2] # constant extrapolation
                pos = x[:(n - i + 1)]
                x_hat[-1] = _linear_extrapolate(pos[-3], x_hat[-3],
                                                pos[-2], x_hat[-2],
                                                pos[-1])
                y_hat = _fft_convolve(x_hat, kernel)
                cv_loss[j] += (y[:(n - i + 1)][-1] - y_hat[-1]) ** 2

    lam = cv_grid[np.argmin(cv_loss)]
    if verbose: print(f"Chosen parameter: {lam:.4}")
    x_hat = fit_func(y=y, x=x, lam=lam)
    return x_hat


def deconvolve_double_smooth_tf_cv(
        y: np.ndarray,
        x: np.ndarray,
        kernel: np.ndarray,
        fit_func: Callable = deconvolve_double_smooth_ntf,
        lam_cv_grid: np.ndarray = np.logspace(1, 3.5, 10),
        gam_cv_grid: np.ndarray = np.r_[np.logspace(0, 0.2, 6) - 1, [1, 5, 10, 50]],
        gam_n_folds: int = 7,
        n_iters: int = 1000,
        k: int = 3,
        clip: bool = True,
        verbose: bool = False) -> np.ndarray:
    """
       Run cross-validation to tune smoothness over deconvolve_double_smooth_ntf.
       First, leave-every-third-out CV is performed over lambda, fixing gamma=0. After
       choosing the lambda with the smallest squared error, forward validation is done to
       select gamma.

       Parameters
       ----------
       y
           array of values to convolve
       x
           array of positions
       kernel
           array with convolution kernel values
       fit_func
           deconvolution function to use
       lam_cv_grid
           grid of trend filtering penalty values to search over
       gam_cv_grid
           grid of second boundary smoothness penalty values to search over
       gam_n_folds
           number of splits for forward cv (see above documentation)
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

    fit_func = partial(fit_func, kernel=kernel, n_iters=n_iters, k=k, clip=clip)
    n = y.shape[0]
    m = kernel.size
    lam_cv_loss = np.zeros((lam_cv_grid.shape[0],))
    gam_cv_loss = np.zeros((gam_cv_grid.shape[0],))

    def _linear_extrapolate(x0, y0, x1, y1, x_new):
        return y0 + ((x_new - x0) / (x1 - x0)) * (y1 - y0)

    # use le3o cv for finding lambda, this controls smoothness of entire curve
    for i in range(3):
        test_split = np.zeros((n,), dtype=bool)
        test_split[i::3] = True
        for j, reg_par in enumerate(lam_cv_grid):
            x_hat = np.full((n,), np.nan)
            x_hat[~test_split] = fit_func(y=y[~test_split], x=x[~test_split],
                                          lam=reg_par, gam=0)
            x_hat = _impute_with_neighbors(x_hat)
            y_hat = _fft_convolve(x_hat, kernel)
            lam_cv_loss[j] += np.sum((y[test_split] - y_hat[test_split]) ** 2)

    lam = lam_cv_grid[np.argmin(lam_cv_loss)]

    # use forward cv to find gamma, this controls smoothness of right-boundary curve
    for i in range(1, gam_n_folds + 1):
        for j, reg_par in enumerate(gam_cv_grid):
            x_hat = np.full((n - i + 1,), np.nan)
            x_hat[:(n - i)] = fit_func(y=y[:(n - i)], x=x[:(n - i)], gam=reg_par, lam=lam)
            pos = x[:(n - i + 1)]
            x_hat[-1] = _linear_extrapolate(pos[-3], x_hat[-3],
                                            pos[-2], x_hat[-2],
                                            pos[-1])
            y_hat = _fft_convolve(x_hat, kernel)
            gam_cv_loss[j] += np.sum((y[:(n - i + 1)][-1:] - y_hat[-1:]) ** 2)

    gam = gam_cv_grid[np.argmin(gam_cv_loss)]
    if verbose: print(f"Chosen parameters: lam:{lam:.4}, gam:{gam:.4}")
    x_hat = fit_func(y=y, x=x, lam=lam, gam=gam)
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
                      fit_func: Callable = deconvolve_double_smooth_tf_cv,
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
            try:
                convolved_truth = convolved_truth.get_data_range(start_date, end_date,
                                                                 "mean")
            except ValueError:
                deconvolved_truths.append(LocationSeries(loc, geo_type))
                continue
            deconvolved_truth = fit_func(y=np.array(convolved_truth),
                                         x=np.arange(1, len(convolved_truth) + 1),
                                         kernel=kernel)
            deconvolved_truths.append(
                LocationSeries(loc,
                               geo_type,
                               dict(zip(full_dates, deconvolved_truth))))
        else:
            # return empty
            deconvolved_truths.append(LocationSeries(loc, geo_type))

        if (j + 1) % 25 == 0: print(f"Deconvolved {j}/{n_locs} locations")

    return deconvolved_truths
