"""
Functions used to calculate direction.

(Thanks to Addison Hu)

Author: Maria Jahja
Created: 2020-04-17

"""

import numpy as np


def running_mean(s):
    """Compute running mean."""
    return np.cumsum(s) / np.arange(1, len(s) + 1)


def running_sd(s, mu=None):
    """
    Compute running standard deviation.

    Running mean can be pre-supplied to save on computation.
    """
    if mu is None:
        mu = running_mean(s)
    sqmu = running_mean(s ** 2)
    sd = np.sqrt(sqmu - mu ** 2)
    return sd


def first_difference_direction(s):
    """
    Declare "notable" increases and decreases.

    Based on the distribution of past first differences.  Code taken from Addison Hu.  Modified to
    return directional strings.

    Args:
        s: input data

    Returns: Directions in "-1", "0", "+1", or "NA" for first 3 values
    """
    T = s[1:] - s[:-1]
    mu = running_mean(T)
    sd = running_sd(T, mu=mu)
    d = np.full(s.shape, "NA")

    for idx in range(2, len(T)):
        if T[idx] < min(mu[idx - 1] - sd[idx - 1], 0):
            d[idx + 1] = "-1"
        elif T[idx] > max(mu[idx - 1] + sd[idx - 1], 0):
            d[idx + 1] = "+1"
        else:
            d[idx + 1] = "0"

    return d
