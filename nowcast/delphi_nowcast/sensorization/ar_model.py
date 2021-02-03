from datetime import datetime, timedelta, date
from typing import Tuple

import numpy as np

from ..data_containers import LocationSeries


def compute_ar_sensor(day: date,
                      values: LocationSeries,
                      ar_size: int,
                      lambda_: float) -> float:
    """
    Fit AR model through least squares and get sensorization value for a given date.

    This takes in a LocationSeries objects for the quantity of interest as well as a date to
    predict and some model parameters. The model is trained on all data before the specified date,
    and then the predictor at the given date is fed into the model to get the returned sensor value
    for that day.

    Missing values are imputed with mean imputation, though currently this function is called
    on data that has no nan values.

    Parameters
    ----------
    day
        date to get sensor value for
    values
        LocationSeries containing covariate values.
    ar_size
        Order of autoregressive model.
    lambda_
        l2 regularization coefficient.

    Returns
    -------
        Float value of sensor on `date`
    """
    previous_day = day - timedelta(1)
    try:
        window = values.get_data_range(min(values.dates), previous_day, "mean")
    except ValueError:
        return np.nan
    B, means, stddevs = _ar_fit(np.array(window), ar_size, lambda_)
    if B is None:
        return np.nan
    date_X = np.hstack((1,
                        (np.array(window[-ar_size:]) - means) / stddevs))
    Yhat = (date_X @ B)[0]
    # Taken from https://github.com/dfarrow0/covidcast-nowcast/blob/dfarrow/sf/src/sf/ar_sensor.py:
    # ground truth in some locations is a zero vector, which leads to perfect AR fit, zero
    # variance, and a singular covariance matrix so as a small hack, add some small noise.
    np.random.seed(int(day.strftime("%Y%m%d")))
    Yhat += np.random.normal(0, 0.1)
    # as a huge hack, add more noise to prevent AR from unreasonably dominating
    # the nowcast since AR3 can nearly exactly predict some trendfiltered curves.
    np.random.seed(int(day.strftime("%Y%m%d")))
    Yhat += np.random.normal(0, 0.1 * np.maximum(0, np.mean(Yhat)))
    return Yhat


def _ar_fit(values: np.array,
            ar_size: int,
            lambda_: float) -> Tuple[np.array, np.array, np.array]:
    """
    Fit AR coefficients with OLS. Standardizes and fits an intercept.

    Adapted from
    https://github.com/dfarrow0/covidcast-nowcast/blob/dfarrow/sf/src/sf/ar_sensor.py

    Parameters
    ----------
    values
        Array of values to train on.
    ar_size
        Order of autoregressive model.
    lambda_
        l2 regularization coefficient.

    Returns
    -------
        Tuple of (fitted coefficients, mean vector, stddev vector).
    """
    num_observations = len(values) - ar_size
    if num_observations < 2 * (ar_size + 1):  # 1 for intercept
        return None, None, None
    X = np.hstack([values[j:-(ar_size - j), None] for j in range(ar_size)])
    X, means, stddevs = _standardize(X)
    Y = values[ar_size:, None]
    B = np.linalg.inv(X.T @ X + lambda_ * np.eye(ar_size)) @ X.T @ Y
    B = np.concatenate(([[np.mean(Y)]], B))
    return B, means, stddevs


def _standardize(data: np.ndarray) -> Tuple[np.ndarray, np.array, np.array]:
    """
    Standardize a matrix and return the mean and stddevs for each column

    Parameters
    ----------
    data
        Numpy matrix to standardize

    Returns
    -------
        Standardize matrix, mean vector, stddev vector.
    """
    means = np.mean(data, axis=0)
    stddevs = np.std(data, axis=0, ddof=1)
    data = (data - means) / stddevs
    return data, means, stddevs
