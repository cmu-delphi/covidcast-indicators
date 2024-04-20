import pandas as pd
import pandas.api.types as ptypes

from delphi_nwss.pull import (
    add_identifier_columns,
    sig_digit_round,
    reformat,
)
from delphi_nwss.constants import TYPE_DICT, TYPE_DICT_METRIC
import numpy as np


def test_sig_digit():
    assert np.isclose(
        sig_digit_round(np.array([12345, 0.01234, 1.2345e-12]), 3),
        np.array([12300, 0.0123, 1.23e-12]),
    ).all()
    assert np.isclose(
        sig_digit_round(np.array([123456789, 0.0123456789, 1.23456789e-12]), 7),
        np.array([123456700, 0.01234567, 1.234567e-12]),
    ).all()


def test_column_conversions_concentration():
    df = pd.read_csv("test_data/conc_data.csv", index_col=0)
    df = df.rename(columns={"date": "timestamp"})
    converted = df.astype(TYPE_DICT)
    assert all(
        converted.columns
        == pd.Index(["key_plot_id", "timestamp", "pcr_conc_smoothed", "normalization"])
    )
    assert ptypes.is_numeric_dtype(converted["pcr_conc_smoothed"])
    assert ptypes.is_datetime64_any_dtype(converted["timestamp"])


def test_column_conversions_metric():
    df = pd.read_csv("test_data/metric_data.csv", index_col=0)
    converted = df.astype(TYPE_DICT_METRIC)
    assert all(
        converted.columns
        == pd.Index(
            [
                "wwtp_jurisdiction",
                "wwtp_id",
                "reporting_jurisdiction",
                "sample_location",
                "key_plot_id",
                "county_names",
                "county_fips",
                "population_served",
                "date_start",
                "date_end",
                "detect_prop_15d",
                "percentile",
                "sampling_prior",
                "first_sample_date",
                "ptc_15d",
                "sample_location_specify",
            ]
        )
    )
    categorical = [
        "wwtp_jurisdiction",
        "reporting_jurisdiction",
        "sample_location",
        "county_names",
        "county_fips",
    ]
    assert all(
        [isinstance(converted[cat].dtype, pd.CategoricalDtype) for cat in categorical]
    )

    float_typed = [
        "population_served",
        "detect_prop_15d",
        "percentile",
        "ptc_15d",
        "sample_location_specify",
    ]
    assert all(ptypes.is_numeric_dtype(converted[flo].dtype) for flo in float_typed)


def test_formatting():
    df_metric = pd.read_csv("test_data/metric_data.csv", index_col=0)
    df_metric = df_metric.astype(TYPE_DICT_METRIC)

    df = pd.read_csv("test_data/conc_data.csv", index_col=0)
    df = df.rename(columns={"date": "timestamp"})
    df = df.astype(TYPE_DICT)

    df_formatted = reformat(df, df_metric)

    assert all(
        df_formatted.columns
        == pd.Index(
            [
                "key_plot_id",
                "timestamp",
                "pcr_conc_smoothed",
                "normalization",
                "population_served",
                "detect_prop_15d",
                "percentile",
                "ptc_15d",
            ]
        )
    )


def test_identifier_colnames():
    test_df = pd.read_csv("test_data/conc_data.csv", index_col=0)
    add_identifier_columns(test_df)
    assert all(test_df.state.unique() == ["ak", "tn"])
    assert all(test_df.provider.unique() == ["CDC_BIOBOT", "WWS"])
    # the only cases where the signal name is wrong is when normalization isn't defined
    assert all(
        (test_df.signal_name == test_df.provider + "_" + test_df.normalization)
        | (test_df.normalization.isna())
    )
    assert all(
        (
            test_df.signal_name.unique()
            == ["CDC_BIOBOT_flow-population", np.nan, "WWS_microbial"]
        )
        | (pd.isna(test_df.signal_name.unique()))
    )
