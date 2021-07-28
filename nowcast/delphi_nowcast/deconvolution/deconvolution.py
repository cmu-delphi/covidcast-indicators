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
                                  norm: bool = False) -> np.ndarray:
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


def criterion(y, x, C, Dk1, D1m, lam, Gam):
    a = 0.5 * np.sum((y - C @ x)**2)
    b = lam * np.sum(np.abs(Dk1 @ x))
    c = x.T @ D1m.T @ Gam @ D1m @ x
    return a + b + c

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

def deconvolve_double_smooth_ntf_fast(
        y: np.ndarray,
        x: np.ndarray,
        kernel: np.ndarray,
        lam: float,
        gam: float,
        n_iters: int = 200,
        k: int = 3,
        clip: bool = False) -> np.ndarray:
    assert k == 3, "Natural TF only implemented for k=3"
    n = y.shape[0]
    m = kernel.shape[0]

    y_sd = np.std(y)
    y_mean = np.mean(y)
    y = (y - y_mean) / y_sd
    rho = lam  # set equal
    C = _construct_convolution_matrix(y, kernel)[:n]
    D = band([-1, 1], [0, 1], shape=(n - 1, n)).toarray()
    D = np.diff(D, n=k, axis=0)
    P = _construct_poly_interp_mat(x, n, k)
    D_m = band([-1, 1], [0, 1], shape=(n - 1, n)).toarray()
    D_m[:-m, :] = 0

    # kernel weights for double smoothing
    weights = np.ones((D_m.shape[0],))
    weights[-m:] = np.cumsum(kernel[::-1])
    weights /= np.max(weights)
    Gam = gam * np.diag(weights)

    # polynomial interpolation
    C = C @ P
    D = D @ P
    D_m = D_m @ P

    # pre-calculations
    Cty = C.T @ y
    first_x_update = np.linalg.inv((2 * D_m.T @ Gam @ D_m) + C.T @ C + rho * D.T @ D)
    alpha_k = np.zeros(n - k - 1)
    u_k = np.zeros(n - k - 1)
    x_k = first_x_update @ (Cty + rho * D.T @ (alpha_k + u_k))
    for i in range(n_iters):
        x_k = first_x_update @ (Cty + rho * D.T @ (alpha_k + u_k))
        alpha_k = dp_1d(D @ x_k, lam / rho)
        u_k = u_k + alpha_k - D @ x_k


    x_k = P @ x_k
    if clip:
        x_k = np.clip(x_k, 0, np.infty)
    return (x_k * y_sd) + y_mean


def deconvolve_double_smooth_tf_cv(
        y: np.ndarray,
        x: np.ndarray,
        kernel: np.ndarray,
        fit_func: Callable = deconvolve_double_smooth_ntf_fast,
        lam_cv_grid: np.ndarray = np.logspace(1, 3.5, 10),
        gam_cv_grid: np.ndarray = np.r_[np.logspace(0, 0.2, 6) - 1, [1, 5, 10, 50]],
        gam_n_folds: int = 7,
        n_iters: int = 200,
        k: int = 3,
        clip: bool = False,
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


def dp_1d(y, lam):
    """Implementation of Nick Johnson's DP solution for 1d fused lasso."""
    n = y.shape[0]
    beta = np.zeros(n)

    # knots
    x = np.zeros((2 * n))
    a = np.zeros((2 * n))
    b = np.zeros((2 * n))

    # knots of back-pointers
    tm = np.zeros((n - 1))
    tp = np.zeros((n - 1))

    # step through first iteration manually
    tm[0] = -lam + y[0]
    tp[0] = lam + y[0]
    l = n - 1
    r = n
    x[l] = tm[0]
    x[r] = tp[0]
    a[l] = 1
    b[l] = -y[0] + lam
    a[r] = -1
    b[r] = y[0] + lam
    afirst = 1
    bfirst = -y[1] - lam
    alast = -1
    blast = y[1] - lam
    # now iterations 2 through n-1
    for k in range(1, n - 1):
        # compute lo
        alo = afirst
        blo = bfirst
        lo = l
        while lo <= r:
            if alo * x[lo] + blo > -lam:
                break
            alo += a[lo]
            blo += b[lo]
            lo += 1

        # compute hi
        ahi = alast
        bhi = blast
        hi = r
        while hi >= lo:
            if (-ahi * x[hi] - bhi) < lam:
                break
            ahi += a[hi]
            bhi += b[hi]
            hi -= 1

        # compute the negative knot
        tm[k] = (-lam - blo) / alo
        l = lo - 1
        x[l] = tm[k]

        # compute the positive knot
        tp[k] = (lam + bhi) / (-ahi)
        r = hi + 1
        x[r] = tp[k]

        # update a and b
        a[l] = alo
        b[l] = blo + lam
        a[r] = ahi
        b[r] = bhi + lam
        afirst = 1
        bfirst = -y[k + 1] - lam
        alast = -1
        blast = y[k + 1] - lam

    # compute the last coefficient - function has zero derivative here
    alo = afirst
    blo = bfirst
    for lo in range(l, r + 1):
        if alo * x[lo] + blo > 0:
            break
        alo += a[lo]
        blo += b[lo]

    beta[n - 1] = -blo / alo

    # compute the rest of the coefficients
    for k in range(n - 2, -1, -1):
        if beta[k + 1] > tp[k]:
            beta[k] = tp[k]
        elif beta[k + 1] < tm[k]:
            beta[k] = tm[k]
        else:
            beta[k] = beta[k + 1]
    return beta



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
