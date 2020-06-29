"""
This file contains a left gauss filter used to smooth a 1-d signal.
Code is courtesy of Addison Hu (minor adjustments by Maria).

Author: Maria Jahja
Created: 2020-04-16

"""
import numpy as np

from .config import Config


def left_gauss_linear(s, h=Config.SMOOTHER_BANDWIDTH):
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
