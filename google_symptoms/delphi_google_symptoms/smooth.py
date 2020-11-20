"""Functions for smoothing signals."""
# -*- coding: utf-8 -*-
import numpy as np

def identity(x):
    """Trivial "smoother" that does no smoothing.

    Parameters
    ----------
    x: np.ndarray
        Input array

    Returns
    -------
    np.ndarray:
        Same as x
    """
    return x

def kday_moving_average(x, k):
    """Compute k-day moving average on x.

    Parameters
    ----------
    x: np.ndarray
        Input array

    Returns
    -------
    np.ndarray:
        k-day moving average.  The first k-1 entries are np.nan.
    """
    if not isinstance(k, int):
        raise ValueError('k must be int.')
    # temp = np.append(np.zeros(k - 1), x)
    temp = np.append(np.nan*np.ones(k-1), x)
    y = np.convolve(temp, np.ones(k, dtype=int), 'valid') / k
    return y
