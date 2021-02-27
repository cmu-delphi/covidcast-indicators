"""
This file contains various filters used to smooth the 1-d signals.

Code is courtesy of Addison Hu (minor adjustments by Maria).

Author: Maria Jahja
Created: 2020-04-16

"""
import numpy as np


def moving_avg(x, y, k=7):
    """Smooth the y-values using a rolling window with k observations.

    Args:
        x: indexing array of the signal
        y: one dimensional signal to smooth
        k: the number of observations to average

    Returns: tuple of indexing array, without the first k-1 obs, and smoothed values
    """
    n = len(y)
    sy = np.zeros((n - k + 1, 1))
    for i in range(len(sy)):
        sy[i] = np.mean(y[i : (i + k)])

    return x[(k - 1) :], sy


def padded_moving_avg(y, k=7):
    """Smooth the y-values using a rolling window with k observations. Pad the first k.

    Args:
        y: one dimensional signal to smooth.
        k: the number of observations to average

    Returns: smoothed values, where the first k-1 obs are padded with 0
    """
    n = len(y)
    sy = np.zeros((n - k + 1, 1))
    for i in range(len(sy)):
        sy[i] = np.mean(y[i : (i + k)])

    # pad first k obs with 0
    for i in range(k - 1):
        sy = np.insert(sy, i, 0)
    return sy.reshape(-1, 1)


def left_gauss(y, h=100):
    """Smooth the y-values using a left Gaussian filter.

    Args:
        y: one dimensional signal to smooth.
        h: smoothing bandwidth (in terms of variance)

    Returns: a smoothed 1D signal.
    """
    t = np.zeros_like(y)
    n = len(t)
    indices = np.arange(n)
    for i in range(1, n):
        wts = np.exp(-(((i - 1) - indices[:i]) ** 2) / h)
        t[i] = np.dot(wts, y[:i]) / np.sum(wts)
    return t


def left_gauss_linear(s, h=250):
    """Smooth the y-values using a local linear left Gaussian filter.

    Args:
        y: one dimensional signal to smooth.
        h: smoothing bandwidth (in terms of variance)

    Returns: a smoothed 1D signal.
    """
    n = len(s)
    t = np.zeros_like(s)
    X = np.vstack([np.ones(n), np.arange(n)]).T
    for idx in range(n):
        wts = np.exp(-((np.arange(idx + 1) - idx) ** 2) / h)
        XwX = np.dot(X[: (idx + 1), :].T * wts, X[: (idx + 1), :])
        Xwy = np.dot(X[: (idx + 1), :].T * wts, s[: (idx + 1)].reshape(-1, 1))
        try:
            beta = np.linalg.solve(XwX, Xwy)
            t[idx] = np.dot(X[: (idx + 1), :], beta)[-1]
        except np.linalg.LinAlgError:
            t[idx] = np.nan
    return t
