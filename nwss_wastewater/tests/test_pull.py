from datetime import datetime, date
import json
from unittest.mock import patch
import tempfile
import os
import time
from datetime import datetime

import pandas as pd
import pandas.api.types as ptypes

from delphi_nwss.pull import construct_typedicts, sig_digit_round, reformat, warn_string
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


def test_column_type_dicts():
    type_dict, type_dict_metric = construct_typedicts()
    assert type_dict == {"pcr_conc_smoothed": float, "timestamp": "datetime64[ns]"}
    assert type_dict_metric == {
        "date_start": "datetime64[ns]",
        "date_end": "datetime64[ns]",
        "detect_prop_15d": float,
        "percentile": float,
        "ptc_15d": float,
        "wwtp_jurisdiction": "category",
        "wwtp_id": int,
        "reporting_jurisdiction": "category",
        "sample_location": "category",
        "county_names": "category",
        "county_fips": "category",
        "population_served": float,
        "sampling_prior": bool,
        "sample_location_specify": float,
    }


def test_column_conversions_concentration():
    type_dict, type_dict_metric = construct_typedicts()
    df = pd.read_csv("test_data/conc_data.csv", index_col=0)
    df = df.rename(columns={"date": "timestamp"})
    converted = df.astype(type_dict)
    assert all(
        converted.columns
        == pd.Index(["key_plot_id", "timestamp", "pcr_conc_smoothed", "normalization"])
    )
    assert ptypes.is_numeric_dtype(converted["pcr_conc_smoothed"])
    assert ptypes.is_datetime64_any_dtype(converted["timestamp"])


def test_column_conversions_metric():
    type_dict, type_dict_metric = construct_typedicts()
    df = pd.read_csv("test_data/metric_data.csv", index_col=0)
    converted = df.astype(type_dict_metric)
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
    type_dict, type_dict_metric = construct_typedicts()
    df_metric = pd.read_csv("test_data/metric_data.csv", index_col=0)
    df_metric = df_metric.astype(type_dict_metric)

    type_dict, type_dict_metric = construct_typedicts()
    df = pd.read_csv("test_data/conc_data.csv", index_col=0)
    df = df.rename(columns={"date": "timestamp"})
    df = df.astype(type_dict)

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
            ]
        )
    )
