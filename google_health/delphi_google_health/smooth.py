# -*- coding: utf-8 -*-
"""Functions for smoothing a signal over time.
"""

import numpy as np
import pandas as pd


def smoothed_values_by_geo_id(df: pd.DataFrame) -> np.ndarray:
    """Computes a smoothed version of the variable 'val' within unique values of 'geo_id'

    Currently uses a local weighted least squares, where the weights are given
    by a Gaussian kernel.

    Parameters
    ----------
    df: pd.DataFrame
        a data frame with columns "geo_id", "timestamp", and "val"

    Returns
    -------
    np.ndarray
        A one-dimensional numpy array containing the smoothed values.
    """
    df = df.copy()
    df["val_smooth"] = 0
    for geo_id in df["geo_id"].unique():
        df.loc[df["geo_id"] == geo_id, "val_smooth"] = _left_gauss_linear(
            s=df[df["geo_id"] == geo_id]["val"].values, h=10, impute=True, minval=0
        )
    return df["val_smooth"].values


def _left_gauss_linear(s: np.ndarray, h=10, impute=False, minval=None) -> np.ndarray:
    """Local weighted least squares, where the weights are given by a Gaussian kernel.

    At each time t, we use the data from times 1, ..., t-dt, weighted
    using the Gaussian kernel, to produce the estimate at time t.

    Parameters
    ----------
    s: np.ndarray
        Input data.  Assumed to be ordered and on an equally spaced grid.
    h: float
        Bandwidth
    impute: bool
        Whether to set the fitted value at idx=0 to s[0].  (The local linear
        estimate is ill-defined for a single data point).
    minval: int
        Enforce a minimum value; for example, used for sensors which are
        nonnegative by definition.

    Returns
    -------
    np.ndarray
        the fitted values
    """

    assert h > 0, "Bandwidth must be positive"

    n = len(s)
    t = np.zeros_like(s, dtype=np.float64)
    X = np.vstack([np.ones(n), np.arange(n)]).T
    for idx in range(n):
        wts = np.exp(-((np.arange(idx + 1) - idx) ** 2) / (h ** 2))
        XwX = np.dot(X[: (idx + 1), :].T * wts, X[: (idx + 1), :])
        Xwy = np.dot(X[: (idx + 1), :].T * wts, s[: (idx + 1)].reshape(-1, 1))
        try:
            beta = np.linalg.solve(XwX, Xwy)
            t[idx] = np.dot(X[: (idx + 1), :], beta)[-1]
        except np.linalg.LinAlgError:
            # At idx 0, method will fail due to rank deficiency.
            t[idx] = s[idx] if impute else np.nan
    if minval is not None:
        t[t <= minval] = minval
    return t

def wip_smoothed_values_by_geo_id(df: pd.DataFrame, p = 2) -> np.ndarray:
    """Computes a smoothed version of the variable 'val' within unique values of 'geo_id'

    Currently uses a local weighted least squares, where the weights are given
    by a Gaussian kernel.

    Parameters
    ----------
    df: pd.DataFrame
        a data frame with columns "geo_id", "timestamp", and "val"
    p: float
        Smoothing window size = 2p + 1

    Returns
    -------
    np.ndarray
        A one-dimensional numpy array containing the smoothed values.
    """
    df = df.copy()
    df["val_smooth"] = 0
    for geo_id in df["geo_id"].unique():
        df.loc[df["geo_id"] == geo_id, "val_smooth"] = _centre_gauss_linear(
            s=df[df["geo_id"] == geo_id]["val"].values, p = p, h=10, impute=True, minval=0
        )
    return df["val_smooth"].values

def gaussian_kernel(x, h):
    """
    x: np.ndarray
        An inreasing sequence with symmetric absolute values.
    p: float
        Smoothing window size = 2p + 1
    """

    return np.exp(-(x ** 2) / (h ** 2))

def _centre_gauss_linear(s: np.ndarray, p = 2, h=10, impute=False, minval=None) -> np.ndarray:
    """Local weighted least squares, where the weights are given by a Gaussian kernel.

    At each time t, we use the data from times 1, ..., t-dt, weighted
    using the Gaussian kernel, to produce the estimate at time t.

    Parameters
    ----------
    s: np.ndarray
        Input data.  Assumed to be ordered and on an equally spaced grid.
    p: float
        Smoothing window size = 2p + 1
    h: float
        Bandwidth
    impute: bool
        Whether to set the fitted value at idx=0 to s[0].  (The local linear
        estimate is ill-defined for a single data point).
    minval: int
        Enforce a minimum value; for example, used for sensors which are
        nonnegative by definition.

    Returns
    -------
    np.ndarray
        the fitted values
    """

    assert h > 0, "Bandwidth must be positive"

    n = len(s)
    t = np.zeros_like(s, dtype=np.float64)
    X = np.vstack([np.ones(n), np.arange(n)]).T
    for idx in range(n):
        kernel_key = np.arange(n) - idx
        wts = gaussian_kernel(kernel_key, h)
        left = max(0, idx-p)
        right = min(n, idx + p+1)
        XwX = np.dot(X[left:right, :].T * wts[left:right], X[left:right, :])
        Xwy = np.dot(X[left:right, :].T * wts[left:right], s[left:right].reshape(-1, 1))
        try:
            beta = np.linalg.solve(XwX, Xwy)
            t[idx] = np.dot(X[left:right, :], beta)[idx - left]
        except np.linalg.LinAlgError:
            # At idx 0, method will fail due to rank deficiency.
            t[idx] = s[idx] if impute else np.nan
    if minval is not None:
        t[t <= minval] = minval
    return t

