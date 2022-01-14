"""Deconvolution functions."""

from functools import partial
from typing import Callable

import numpy as np
from scipy.linalg import toeplitz
from scipy.sparse import diags as band


def deconvolve_double_smooth_ntf(
        y: np.ndarray,
        x: np.ndarray,
        kernel: np.ndarray,
        lam: float,
        gam: float,
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
    P = _construct_poly_interp_mat(x, k)
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
    for _ in range(n_iters):
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


def deconvolve_double_smooth_tf_cv(
        y: np.ndarray,
        x: np.ndarray,
        kernel: np.ndarray,
        fit_func: Callable = deconvolve_double_smooth_ntf,
        lam_cv_grid: np.ndarray = np.logspace(1, 3.5, 10),
        gam_cv_grid: np.ndarray = np.r_[np.logspace(0, 0.2, 6) - 1, [1, 5, 10, 50]],
        gam_n_folds: int = 10,
        n_iters: int = 200,
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
    lam_cv_loss = np.zeros((lam_cv_grid.shape[0],))
    gam_cv_loss = np.zeros((gam_cv_grid.shape[0],))

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
    if verbose:
        print(f"Chosen parameters: lam:{lam:.4}, gam:{gam:.4}")
    x_hat = fit_func(y=y, x=x, lam=lam, gam=gam)
    return x_hat


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
    padding = np.zeros(n - 1)
    first_col = np.r_[kernel, padding]
    first_row = np.r_[kernel[0], padding]
    P = toeplitz(first_col, first_row)
    if norm:
        scale = P.sum(axis=1) / kernel.sum()
        return P / scale[:, np.newaxis]
    return P


def _soft_thresh(x: np.ndarray, lam: float) -> np.ndarray:
    """Perform soft-thresholding of x with threshold lam."""
    return np.sign(x) * np.maximum(np.abs(x) - lam, 0)


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


def _impute_with_neighbors(x: np.ndarray) -> np.ndarray:
    """
    Impute missing values with the average of the elements immediately
    before and after.

    Parameters
    ----------
    x
        Signal with missing values.

    Returns
    -------
        Imputed signal.
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


def _construct_poly_interp_mat(x: np.ndarray, k: int = 3):
    """
    Generate polynomial interpolation matrix.

    Currently only implemented for 3rd order polynomials.

    Parameters
    ----------
    x
        Input signal.
    k
        Order of the polynomial interpolation.

    Returns
    -------
        n x (n - k - 1) matrix.
    """
    assert k == 3, "poly interpolation matrix only constructed for k=3"
    n = x.shape[0]
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


def _linear_extrapolate(x0, y0, x1, y1, x_new):
    """Linearly extrapolate the value at x_new from 2 given points (x0, y0) and (x1, y1)."""
    return y0 + ((x_new - x0) / (x1 - x0)) * (y1 - y0)
