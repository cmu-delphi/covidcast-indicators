# -*- coding: utf-8 -*-
"""Functions for pulling NCHS mortality data API."""

import numpy as np
import pandas as pd
from sodapy import Socrata

from .constants import (
    SIGNALS,
    METRIC_SIGNALS,
    METRIC_DATES,
    SAMPLE_SITE_NAMES,
    SIG_DIGITS,
    NEWLINE,
)


def sig_digit_round(value, n_digits):
    """Truncate value to only n_digits.

    Truncate precision of elements in `value` (a numpy array) to the specified number of
    significant digits (9.87e5 would be 3 sig digits).
    """
    in_value = value
    value = np.asarray(value).copy()
    zero_mask = (value == 0) | np.isinf(value) | np.isnan(value)
    value[zero_mask] = 1.0
    sign_mask = value < 0
    value[sign_mask] *= -1
    exponent = np.ceil(np.log10(value))
    result = 10**exponent * np.round(value * 10 ** (-exponent), n_digits)
    result[sign_mask] *= -1
    result[zero_mask] = in_value[zero_mask]
    return result


def construct_typedicts():
    """Create the type conversion dictionary for both dataframes."""
    # basic type conversion
    type_dict = {key: float for key in SIGNALS}
    type_dict["timestamp"] = "datetime64[ns]"
    # metric type conversion
    signals_dict_metric = {key: float for key in METRIC_SIGNALS}
    metric_dates_dict = {key: "datetime64[ns]" for key in METRIC_DATES}
    type_dict_metric = {**metric_dates_dict, **signals_dict_metric, **SAMPLE_SITE_NAMES}
    return type_dict, type_dict_metric


def warn_string(df, type_dict):
    """Format the warning string."""
    return f"""
Expected column(s) missed, The dataset schema may
have changed. Please investigate and amend the code.

Columns needed:
{NEWLINE.join(sorted(type_dict.keys()))}

Columns available:
{NEWLINE.join(sorted(df.columns))}
"""


def add_population(df, df_metric):
    """Add the population column from df_metric to df, and rename some columns."""
    # drop unused columns from df_metric
    df_population = df_metric.loc[:, ["key_plot_id", "date_start", "population_served"]]
    # get matching keys
    df_population = df_population.rename(columns={"date_start": "timestamp"})
    df_population = df_population.set_index(["key_plot_id", "timestamp"])
    df = df.set_index(["key_plot_id", "timestamp"])

    df = df.join(df_population)
    df = df.reset_index()
    return df


def pull_nwss_data(socrata_token: str):
    """Pull the latest NWSS Wastewater data, and conforms it into a dataset.

    The output dataset has:

    - Each row corresponds to (sewershed_key, day), denoted (geo_id, timestamp)
    - Each row additionally has columns 'pcr_conc_smoothed', 'normalization',
    'population_served', 'ptc_15d', 'detect_prop_15d',

    Parameters
    ----------
    socrata_token: str
        My App Token for pulling the NWSS data (could be the same as the nchs data)
    test_file: Optional[str]
        When not null, name of file from which to read test data

    Returns
    -------
    pd.DataFrame
        Dataframe as described above.
    """
    # concentration key types
    type_dict, type_dict_metric = construct_typedicts()

    # Pull data from Socrata API
    client = Socrata("data.cdc.gov", socrata_token)
    results_concentration = client.get("g653-rqe2", limit=10**10)
    results_metric = client.get("2ew6-ywp6", limit=10**10)
    df_metric = pd.DataFrame.from_records(results_metric)
    df_concentration = pd.DataFrame.from_records(results_concentration)
    df_concentration = df_concentration.rename(columns={"date": "timestamp"})

    try:
        df_concentration = df_concentration.astype(type_dict)
    except KeyError as exc:
        raise ValueError(warn_string(df_concentration, type_dict)) from exc

    try:
        df_metric = df_metric.astype(type_dict_metric)
    except KeyError as exc:
        raise ValueError(warn_string(df_metric, type_dict_metric)) from exc

    # pull 2 letter state labels out of the key_plot_id labels
    df_concentration["state"] = df_concentration.key_plot_id.str.extract(r"_(\w\w)_")

    # round out some of the numeric noise that comes from smoothing
    df_concentration[SIGNALS[0]] = sig_digit_round(
        df_concentration[SIGNALS[0]], SIG_DIGITS
    )

    df_concentration = add_population(df_concentration, df_metric)
    # if there are population NA's, assume the previous value is accurate (most
    # likely introduced by dates only present in one and not the other; even
    # otherwise, best to assume some value rather than break the data)
    df_concentration.population_served = df_concentration.population_served.ffill()

    keep_columns = ["timestamp", "state", "population_served"]
    return df_concentration[SIGNALS + keep_columns]
