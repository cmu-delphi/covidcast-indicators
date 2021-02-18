"""
This file contains a left gauss filter used to smooth a 1-d signal.

Code is courtesy of Addison Hu (minor adjustments by Maria).

Author: Maria Jahja
Created: 2020-04-16
Modified: 2020-09-27
    - partially concede few naming changes for pylint

"""
import numpy as np

from .config import Config


def left_gauss_linear(arr, bandwidth=Config.SMOOTHER_BANDWIDTH):
    """
    Smooth the y-values using a local linear left Gaussian filter.

    Args:
        arr: one dimensional signal to smooth.
        bandwidth: smoothing bandwidth (in terms of variance)

    Returns: a smoothed 1D signal.

    """
    n_rows = len(arr)
    out_arr = np.zeros_like(arr)
    X = np.vstack([np.ones(n_rows), np.arange(n_rows)]).T  # pylint: disable=invalid-name
    for idx in range(n_rows):
        weights = np.exp(-((np.arange(idx + 1) - idx) ** 2) / bandwidth)
        # pylint: disable=invalid-name
        XwX = np.dot(X[: (idx + 1), :].T * weights, X[: (idx + 1), :])
        Xwy = np.dot(X[: (idx + 1), :].T * weights, arr[: (idx + 1)].reshape(-1, 1))
        # pylint: enable=invalid-name
        try:
            beta = np.linalg.solve(XwX, Xwy)
            out_arr[idx] = np.dot(X[: (idx + 1), :], beta)[-1]
        except np.linalg.LinAlgError:
            out_arr[idx] = np.nan
    return out_arr
