"""
Functions to calculate the quidel sensor statistic.
"""

import numpy as np
import pandas as pd

def _prop_var(p, n):
    """var(X/n) = 1/(n^2)var(X) = (npq)/(n^2) = pq/n"""
    return p * (1 - p) / n

def fill_dates(y_data, first_date, last_date):
    """
    Ensure all dates are listed in the data, otherwise, add days with 0 counts.
    Args:
        y_data: dataframe with datetime index
        first_date: datetime.datetime
            first date to be included
        last_date: datetime.datetime
            last date to be inclluded
    Returns: dataframe containing all dates given
    """
    cols = y_data.columns
    if first_date not in y_data.index:
        y_data = y_data.append(pd.DataFrame(dict.fromkeys(cols, 0.),
                                            columns=cols, index=[first_date]))
    if last_date not in y_data.index:
        y_data = y_data.append(pd.DataFrame(dict.fromkeys(cols, 0.),
                                            columns=cols, index=[last_date]))

    y_data.sort_index(inplace=True)
    y_data = y_data.asfreq('D', fill_value=0)
    y_data.fillna(0, inplace=True)
    return y_data


def _slide_window_sum(arr, k):
    """
    Sliding window sum, with fixed window size k.  For indices 0:k, we
    DO compute a sum, using whatever points are available.

    Reference: https://stackoverflow.com/a/38507725

    Args:
        arr: np.ndarray
            Array over which to calculate sliding window sum
        k: int
            Window size

    Returns:
        sarr: np.ndarray
            Array of same length of arr, holding the sliding window sum.
    """

    if not isinstance(k, int):
        raise ValueError('k must be int.')
    temp = np.append(np.zeros(k - 1), arr)
    sarr = np.convolve(temp, np.ones(k, dtype=int), 'valid')
    return sarr


def _geographical_pooling(tpooled_tests, tpooled_ptests, min_obs):
    """
    Calculates the proportion of parent samples (tests) that must be "borrowed"
    in order to properly compute the statistic.  If there are no samples
    available in the parent, the borrow_prop is 0.  If the parent does not
    have enough samples, we return a borrow_prop of 1, and the fact that the
    pooled samples are insufficient are handled in the statistic fitting
    step.

    Args:
        tpooled_tests: np.ndarray[float]
            Number of tests after temporal pooling.
            There should be no np.nan here.
        tpooled_ptests: np.ndarray[float]
            Number of parent tests after temporal pooling.
            There should be no np.nan here.
        min_obs: int
            Minimum number of observations in order to compute a ratio
    Returns:
        np.ndarray[float]
            Same length as tests; proportion of parent observations to borrow.
    """
    if (np.any(np.isnan(tpooled_tests)) or np.any(np.isnan(tpooled_ptests))):
        print(tpooled_tests)
        print(tpooled_ptests)
        raise ValueError('[parent] tests should be non-negative '
                         'with no np.nan')
    # STEP 1: "TOP UP" USING PARENT LOCATION
    # Number of observations we need to borrow to "top up"
    borrow_tests = np.maximum(min_obs - tpooled_tests, 0)
    # There are many cases (a, b > 0):
    # Case 1: a / b => no problem
    # Case 2: a / 0 => np.inf => borrow_prop becomes 1
    # Case 3: 0 / a => no problem
    # Case 4: 0 /0 => np.nan => 0 this can happen when a
    # region has enough observations but its parent has nothing.
    # We ignore RuntimeWarnings and handle them ourselves.
    # Reference: https://stackoverflow.com/a/29950752
    with np.errstate(divide='ignore', invalid='ignore'):
        borrow_prop = borrow_tests / tpooled_ptests
        # If there's nothing to borrow, then ya can't borrow
        borrow_prop[np.isnan(borrow_prop)] = 0
        # Can't borrow more than total no. observations.
        # Relies on the fact that np.inf > 1
        borrow_prop[borrow_prop > 1] = 1
    return borrow_prop


def raw_positive_prop(positives, tests, min_obs):
    """
    Calculates the proportion of positive tests for a single geographic
    location, without any temporal smoothing.

    If on any day t, tests[t] < min_obs, then we report np.nan.

    The second and third returned np.ndarray are the standard errors,
    calculated using the binomial proportion variance _prop_var(); and
    the sample size.

    Args:
        positives: np.ndarray[float]
            Number of positive tests, ordered in time, where each array element
            represents a subsequent day.  If there were no positive tests or
            there were no tests performed, this should be zero (never np.nan).
        tests: np.ndarray[float]
            Number of tests performed.  If there were no tests performed, this
            should be zero (never np.nan).  We should always have
            positive[t] <= tests[t] for all t.
        min_obs: int
            Minimum number of observations in order to compute a proportion.
        pool_days: int
            Will not be used, just to keep the format the same for raw and smoothed
    Returns:
        np.ndarray
            Proportion of positive tests on each day, with the same length
            as positives and tests.
        np.ndarray
            Standard errors, calculated using the usual binomial variance.
            Of the same length as above.
        np.ndarray
            Sample size used to compute estimates.
    """
    positives = positives.astype(float)
    tests = tests.astype(float)
    if np.any(np.isnan(positives)) or np.any(np.isnan(tests)):
        print(positives, tests)
        raise ValueError('positives and tests should be non-negative '
                         'with no np.nan')
    if np.any(positives > tests):
        raise ValueError('positives should not exceed tests')
    if min_obs <= 0:
        raise ValueError('min_obs should be positive')
    # nan out any days where there are insufficient observations
    # this also elegantly sidesteps 0/0 division.
    tests[tests < min_obs] = np.nan
    positive_prop = positives / tests
    se = np.sqrt(_prop_var(positive_prop, tests))
    sample_size = tests
    return positive_prop, se, sample_size


def smoothed_positive_prop(positives, tests, min_obs, pool_days,
                           parent_positives=None, parent_tests=None):
    """
    Calculates the proportion of negative tests for a single geographic
    location, with temporal smoothing.

    For a given day t, if sum(tests[(t-pool_days+1):(t+1)]) < min_obs, then we
    'borrow' min_obs - sum(tests[(t-pool_days+1):(t+1)]) observations from the
    parents over the same timespan.  Importantly, it will make sure NOT to
    borrow observations that are _already_ in the current geographic partition
    being considered.

    If min_obs is specified but not satisfied over the pool_days, and
    parent arrays are not provided, then we report np.nan.

    The second and third returned np.ndarray are the standard errors,
    calculated using the binomial proportion variance _prop_var(); and
    the reported sample_size.

    Args:
        positives: np.ndarray[float]
            Number of positive tests, ordered in time, where each array element
            represents a subsequent day.  If there were no positive tests or
            there were no tests performed, this should be zero (never np.nan).
        tests: np.ndarray[float]
            Number of tests performed.  If there were no tests performed, this
            should be zero (never np.nan).  We should always have
            positives[t] <= tests[t] for all t.
        min_obs: int
            Minimum number of observations in order to compute a proportion.
        pool_days: int
            Number of days in the past (including today) over which to pool data.
        parent_positives: np.ndarray
            Like positives, but for the parent geographic partition (e.g., State)
            If this is None, then this shall have 0 positives uniformly.
        parent_tests: np.ndarray
            Like tests, but for the parent geographic partition (e.g., State)
            If this is None, then this shall have 0 tests uniformly.

    Returns:
        np.ndarray
            Proportion of positive tests after the pool_days pooling, with the same
            length as positives and tests.
        np.ndarray
            Standard errors, calculated using the usual binomial variance.
            Of the same length as above.
        np.ndarray
            Effective sample size (after temporal and geographic pooling).
    """

    positives = positives.astype(float)
    tests = tests.astype(float)
    if (parent_positives is None) or (parent_tests is None):
        has_parent = False
    else:
        has_parent = True
        parent_positives = parent_positives.astype(float)
        parent_tests = parent_tests.astype(float)
    if np.any(np.isnan(positives)) or np.any(np.isnan(tests)):
        raise ValueError('positives and tests '
                         'should be non-negative with no np.nan')
    if np.any(positives > tests):
        raise ValueError('positives should not exceed tests')
    if has_parent:
        if np.any(np.isnan(parent_positives)) or np.any(np.isnan(parent_tests)):
            raise ValueError('parent positives and parent tests '
                             'should be non-negative with no np.nan')
        if np.any(parent_positives > parent_tests):
            raise ValueError('positives should not exceed tests')
    if min_obs <= 0:
        raise ValueError('min_obs should be positive')
    if (pool_days <= 0) or not isinstance(pool_days, int):
        raise ValueError('pool_days should be a positive int')

    # STEP 0: DO THE TEMPORAL POOLING
    tpooled_positives = _slide_window_sum(positives, pool_days)
    tpooled_tests = _slide_window_sum(tests, pool_days)
    if has_parent:
        tpooled_ppositives = _slide_window_sum(parent_positives, pool_days)
        tpooled_ptests = _slide_window_sum(parent_tests, pool_days)
        borrow_prop = _geographical_pooling(tpooled_tests, tpooled_ptests, min_obs)
        pooled_positives = (tpooled_positives
                            + borrow_prop * tpooled_ppositives)
        pooled_tests = (tpooled_tests
                        + borrow_prop * tpooled_ptests)
    else:
        pooled_positives = tpooled_positives
        pooled_tests = tpooled_tests
    ## STEP 2: CALCULATE AS THOUGH THEY'RE RAW
    return raw_positive_prop(pooled_positives, pooled_tests, min_obs)


def raw_tests_per_device(devices, tests, min_obs):
    '''
    Calculates the tests per device for a single geographic
    location, without any temporal smoothing.

    If on any day t, tests[t] < min_obs, then we report np.nan.
    The second and third returned np.ndarray are the standard errors,
    currently all np.nan; and the sample size.
    Args:
        devices: np.ndarray[float]
            Number of devices, ordered in time, where each array element
            represents a subsequent day.  If there were no devices, this should
            be zero (never np.nan).
        tests: np.ndarray[float]
            Number of tests performed.  If there were no tests performed, this
            should be zero (never np.nan).
        min_obs: int
            Minimum number of observations in order to compute a ratio
    Returns:
        np.ndarray
            Tests per device on each day, with the same length
            as devices and tests.
        np.ndarray
            Placeholder for standard errors
        np.ndarray
            Sample size used to compute estimates.
    '''
    devices = devices.astype(float)
    tests = tests.astype(float)
    if (np.any(np.isnan(devices)) or np.any(np.isnan(tests))):
        print(devices)
        print(tests)
        raise ValueError('devices and tests should be non-negative '
                         'with no np.nan')
    if min_obs <= 0:
        raise ValueError('min_obs should be positive')
    tests[tests < min_obs] = np.nan
    tests_per_device = tests / devices
    se = np.repeat(np.nan, len(devices))
    sample_size = tests

    return tests_per_device, se, sample_size

def smoothed_tests_per_device(devices, tests, min_obs, pool_days,
                              parent_devices=None, parent_tests=None):
    """
    Calculates the ratio of tests per device for a single geographic
    location, with temporal smoothing.
    For a given day t, if sum(tests[(t-pool_days+1):(t+1)]) < min_obs, then we
    'borrow' min_obs - sum(tests[(t-pool_days+1):(t+1)]) observations from the
    parents over the same timespan.  Importantly, it will make sure NOT to
    borrow observations that are _already_ in the current geographic partition
    being considered.
    If min_obs is specified but not satisfied over the pool_days, and
    parent arrays are not provided, then we report np.nan.
    The second and third returned np.ndarray are the standard errors,
    currently all placeholder np.nan; and the reported sample_size.
    Args:
        devices: np.ndarray[float]
            Number of devices, ordered in time, where each array element
            represents a subsequent day.  If there were no devices, this should
            be zero (never np.nan).
        tests: np.ndarray[float]
            Number of tests performed.  If there were no tests performed, this
            should be zero (never np.nan).
        min_obs: int
            Minimum number of observations in order to compute a ratio
        pool_days: int
            Number of days in the past (including today) over which to pool data.
        parent_devices: np.ndarray
            Like devices, but for the parent geographic partition (e.g., State)
            If this is None, then this shall have 0 devices uniformly.
        parent_tests: np.ndarray
            Like tests, but for the parent geographic partition (e.g., State)
            If this is None, then this shall have 0 tests uniformly.
    Returns:
        np.ndarray
            Tests per device after the pool_days pooling, with the same
            length as devices and tests.
        np.ndarray
            Standard errors, currently uniformly np.nan (placeholder).
        np.ndarray
            Effective sample size (after temporal and geographic pooling).
    """
    devices = devices.astype(float)
    tests = tests.astype(float)
    if (parent_devices is None) or (parent_tests is None):
        has_parent = False
    else:
        has_parent = True
        parent_devices = parent_devices.astype(float)
        parent_tests = parent_tests.astype(float)
    if (np.any(np.isnan(devices)) or np.any(np.isnan(tests))):
        raise ValueError('devices and tests '
                         'should be non-negative with no np.nan')
    if has_parent:
        if (np.any(np.isnan(parent_devices))
            or np.any(np.isnan(parent_tests))):
            raise ValueError('parent devices and parent tests '
                       'should be non-negative with no np.nan')
    if min_obs <= 0:
        raise ValueError('min_obs should be positive')
    if (pool_days <= 0) or not isinstance(pool_days, int):
        raise ValueError('pool_days should be a positive int')
    # STEP 0: DO THE TEMPORAL POOLING
    tpooled_devices = _slide_window_sum(devices, pool_days)
    tpooled_tests = _slide_window_sum(tests, pool_days)
    if has_parent:
        tpooled_pdevices = _slide_window_sum(parent_devices, pool_days)
        tpooled_ptests = _slide_window_sum(parent_tests, pool_days)
        borrow_prop = _geographical_pooling(tpooled_tests, tpooled_ptests,
                                            min_obs)
        pooled_devices = (tpooled_devices
                          + borrow_prop * tpooled_pdevices)
        pooled_tests = (tpooled_tests
                        + borrow_prop * tpooled_ptests)
    else:
        pooled_devices = tpooled_devices
        pooled_tests = tpooled_tests
    ## STEP 2: CALCULATE AS THOUGH THEY'RE RAW
    return raw_tests_per_device(pooled_devices, pooled_tests, min_obs)
