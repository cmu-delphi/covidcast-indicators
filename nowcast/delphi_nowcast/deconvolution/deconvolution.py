"""Deconvolution functions."""

from functools import partial
from typing import Callable

import numpy as np
from scipy.linalg import toeplitz
from scipy.sparse import diags as band


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
