from datetime import datetime, timedelta, date

import numpy as np

from ..data_containers import LocationSeries

MIN_SAMPLE_SIZE = 5  # arbitrarily chosen for now.


def compute_regression_sensor(day: date,
                              covariate: LocationSeries,
                              response: LocationSeries,
                              include_intercept: bool) -> float:
    """
    Fit regression model and get sensorization value for a given date.

    This takes two LocationSeries objects for a covariate and response as well as a date to
    predict and some model parameters. The model is trained on all data before the specified date,
    and then the predictor at the given date is fed into the model to get the returned sensor value
    for that day.

    For now, this function assumes there are no gaps in the data.

    It does not normalize the data yet.

    Parameters
    ----------
    day
        date to get sensor value for
    covariate
        LocationSeries containing covariate values.
    response
        LocationSeries containing response values.
    include_intercept
        Boolean on whether or not to include intercept.

    Returns
    -------
        Float value of sensor on `date`
    """
    previous_day = day - timedelta(1)
    try:
        first_day = max(min(covariate.dates), min(response.dates))
        train_Y = response.get_data_range(first_day, previous_day)
        train_covariates = covariate.get_data_range(first_day, previous_day)
    except ValueError:
        return np.nan
    if not train_Y:
        return np.nan
    non_nan_values = [(i, j) for i, j in zip(train_Y, train_covariates)
                      if not (np.isnan(i) or np.isnan(j))]
    train_Y, train_covariates = zip(*non_nan_values) if non_nan_values else ([], [])
    if len(train_Y) < MIN_SAMPLE_SIZE:
        print("insufficient observations")
        return np.nan
    train_Y = np.array(train_Y)
    train_covariates = np.array(train_covariates)
    X = np.ones((len(train_covariates), 1 + include_intercept))
    X[:, -1] = train_covariates
    B = np.linalg.inv(X.T @ X) @ X.T @ train_Y
    date_val = covariate.data.get(day, np.nan)
    date_X = np.array((1, date_val)) if include_intercept else np.array([date_val])
    return date_X @ B
