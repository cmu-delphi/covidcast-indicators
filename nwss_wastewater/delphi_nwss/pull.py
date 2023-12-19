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
    """Round the number of significant digits (x.xxe5 would be 3) to `N`."""
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
    signals_dict = {key: float for key in SIGNALS}
    type_dict = {**signals_dict}
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
{NEWLINE.join(type_dict.keys())}

Columns available:
{NEWLINE.join(df.columns)}
"""


def reformat(df, df_metric):
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


def pull_nwss_data(token: str):
    """Pull the latest NWSS Wastewater data, and conforms it into a dataset.

    The output dataset has:

    - Each row corresponds to (sewershed_key, day), denoted (geo_id, timestamp)
    - Each row additionally has columns 'pcr_conc_smoothed', 'normalization',
    'population_served', 'ptc_15d', 'detect_prop_15d',

    Parameters
    ----------
    token: str
        My App Token for pulling the NWSS data (could be the same as the nchs data)
    test_file: Optional[str]
        When not null, name of file from which to read test data

    Returns
    -------
    pd.DataFrame
        Dataframe as described above.
    """
    # Constants
    keep_columns = SIGNALS.copy()
    # concentration key types
    type_dict, type_dict_metric = construct_typedicts()

    # Pull data from Socrata API
    client = Socrata("data.cdc.gov", token)
    results_concentration = client.get("g653-rqe2", limit=10**10)
    results_metric = client.get("2ew6-ywp6", limit=10**10)
    df_metric = pd.DataFrame.from_records(results_metric)
    df = pd.DataFrame.from_records(results_concentration)
    df = df.rename(columns={"date": "timestamp"})

    try:
        df = df.astype(type_dict)
    except KeyError as exc:
        raise ValueError(warn_string(df, type_dict)) from exc

    try:
        df_metric = df_metric.astype(type_dict_metric)
    except KeyError as exc:
        raise ValueError(warn_string(df_metric, type_dict_metric)) from exc

    # pull 2 letter state labels out of the key_plot_id labels
    df["state"] = df.key_plot_id.str.extract(r"_(\w\w)_")

    # round out some of the numeric noise that comes from smoothing
    for signal in SIGNALS:
        df[signal] = sig_digit_round(df[signal], SIG_DIGITS)

    df = reformat(df, df_metric)
    # if there are population NA's, assume the previous value is accurate (most
    # likely introduced by dates only present in one and not the other; even
    # otherwise, best to assume some value rather than break the data)
    df.population_served = df.population_served.ffill()

    keep_columns.extend(["timestamp", "state", "population_served"])
    return df[keep_columns]
