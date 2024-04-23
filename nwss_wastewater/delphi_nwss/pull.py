# -*- coding: utf-8 -*-
"""Functions for pulling NCHS mortality data API."""

import numpy as np
import pandas as pd
from sodapy import Socrata

from .constants import (
    SIGNALS,
    PROVIDER_NORMS,
    METRIC_SIGNALS,
    SIG_DIGITS,
    TYPE_DICT,
    TYPE_DICT_METRIC,
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
    result = 10 ** exponent * np.round(value * 10 ** (-exponent), n_digits)
    result[sign_mask] *= -1
    result[zero_mask] = in_value[zero_mask]
    return result


def convert_df_type(df, type_dict, logger):
    """Convert types and warn if there are unexpected columns."""
    try:
        df = df.astype(type_dict)
    except KeyError as exc:
        newline = "\n"
        raise KeyError(
            f"""
Expected column(s) missed, The dataset schema may
have changed. Please investigate and amend the code.

expected={newline.join(sorted(type_dict.keys()))}

received={newline.join(sorted(df.columns))}
"""
        ) from exc
    if new_columns := set(df.columns) - set(type_dict.keys()):
        logger.info("New columns found in NWSS dataset.", new_columns=new_columns)
    return df


def reformat(df, df_metric):
    """Add columns from df_metric to df, and rename some columns.

    Specifically the population and METRIC_SIGNAL columns, and renames date_start to timestamp.
    """
    # drop unused columns from df_metric
    df_metric_core = df_metric.loc[
        :, ["key_plot_id", "date_end", "population_served", *METRIC_SIGNALS]
    ]
    # get matching keys
    df_metric_core = df_metric_core.rename(columns={"date_end": "timestamp"})
    df_metric_core = df_metric_core.set_index(["key_plot_id", "timestamp"])
    df = df.set_index(["key_plot_id", "timestamp"])

    df = df.join(df_metric_core)
    df = df.reset_index()
    return df


def add_identifier_columns(df):
    """Add identifier columns.

    Add columns to get more detail than key_plot_id gives;
    specifically, state, and `provider_normalization`, which gives the signal identifier
    """
    # a pair of alphanumerics surrounded by _
    df["state"] = df.key_plot_id.str.extract(r"_(\w\w)_")
    # anything followed by state ^
    df["provider"] = df.key_plot_id.str.extract(r"(.*)_[a-z]{2}_")
    df["signal_name"] = df.provider + "_" + df.normalization


def check_endpoints(df):
    """Make sure that there aren't any new signals that we need to add."""
    # compare with existing column name checker
    # also add a note about handling errors
    unique_provider_norms = (
        df[["provider", "normalization"]]
        .drop_duplicates()
        .sort_values(["provider", "normalization"])
        .reset_index(drop=True)
    )
    if not unique_provider_norms.equals(pd.DataFrame(PROVIDER_NORMS)):
        raise ValueError(
            f"There are new providers and/or norms. They are\n{unique_provider_norms}"
        )


def pull_nwss_data(token: str, logger):
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
    # Pull data from Socrata API
    client = Socrata("data.cdc.gov", token)
    results_concentration = client.get("g653-rqe2", limit=10 ** 10)
    results_metric = client.get("2ew6-ywp6", limit=10 ** 10)
    df_metric = pd.DataFrame.from_records(results_metric)
    df_concentration = pd.DataFrame.from_records(results_concentration)
    df_concentration = df_concentration.rename(columns={"date": "timestamp"})

    # Schema checks.
    df_concentration = convert_df_type(df_concentration, TYPE_DICT, logger)
    df_metric = convert_df_type(df_metric, TYPE_DICT_METRIC, logger)

    # Drop sites without a normalization scheme.
    df = df_concentration[~df_concentration["normalization"].isna()]

    # Pull 2 letter state labels out of the key_plot_id labels.
    add_identifier_columns(df)

    # move population and metric signals over to df
    df = reformat(df, df_metric)
    # round out some of the numeric noise that comes from smoothing
    for signal in [*SIGNALS, *METRIC_SIGNALS]:
        df[signal] = sig_digit_round(df[signal], SIG_DIGITS)

    # if there are population NA's, assume the previous value is accurate (most
    # likely introduced by dates only present in one and not the other; even
    # otherwise, best to assume some value rather than break the data)
    df.population_served = df.population_served.ffill()
    check_endpoints(df)

    keep_columns = [
        *SIGNALS,
        *METRIC_SIGNALS,
        "timestamp",
        "state",
        "population_served",
        "normalization",
        "provider",
    ]
    return df[keep_columns]
