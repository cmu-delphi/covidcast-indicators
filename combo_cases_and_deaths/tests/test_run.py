"""Tests for running combo cases and deaths indicator."""
from datetime import date
from itertools import product
import unittest
from unittest.mock import patch, call
import pandas as pd
import numpy as np

from delphi_combo_cases_and_deaths.run import (
    add_nancodes, extend_raw_date_range,
    get_updated_dates,
    sensor_signal,
    combine_usafacts_and_jhu,
    compute_special_geo_dfs,
    COLUMN_MAPPING)
from delphi_combo_cases_and_deaths.constants import METRICS, SMOOTH_TYPES, SENSORS
from delphi_utils import Nans


def test_issue_dates():
    """The smoothed value for a particular date is computed from the raw
    values for a span of dates. We want users to be able to see in the
    API all the raw values that went into the smoothed computation,
    for transparency and peer review. This means that each issue
    should contain more days of raw data than smoothed data.
    """
    reference_dr = [date.today(), date.today()]
    params = {'indicator': {'date_range': reference_dr}}
    n_changed = 0
    variants = [sensor_signal(metric, sensor, smoother) for
                metric, sensor, smoother in
                product(METRICS, SENSORS, SMOOTH_TYPES)]
    variants_changed = []
    for sensor_name, _ in variants:
        dr = extend_raw_date_range(params, sensor_name)
        if dr[0] != reference_dr[0]:
            n_changed += 1
            variants_changed.append(sensor_name)
    assert n_changed == len(variants) / 2, f"""
Raw variants should post more days than smoothed.
All variants: {variants}
Date-extended variants: {variants_changed}
"""

@patch("covidcast.covidcast.signal")
def test_unstable_sources(mock_covidcast_signal):
    """Verify that combine_usafacts_and_jhu assembles the combined data
    frame correctly for all cases where 0, 1, or both signals are
    available.
    """
    date_count = [1]
    def jhu(geo, c=date_count):
        if geo == "state":
            geo_val = "pr"
        elif geo == "msa":
            geo_val = "38660"
        else:
            geo_val = "72001"
        return pd.DataFrame(
            [(date.fromordinal(c[0]),geo_val,1,1,1)],
            columns="timestamp geo_value value stderr sample_size".split())
    def uf(geo, c=date_count):
        if geo == "state":
            geo_val = "ny"
        elif geo == "msa":
            geo_val = "10580"
        else:
            geo_val = "36001"
        return pd.DataFrame(
            [(date.fromordinal(c[0]),geo_val,1,1,1)],
            columns="timestamp geo_value value stderr sample_size".split())
    def make_mock(geo):
        # The first two in each row provide a unique_date array of the appropriate length for
        # query of the latter two (in combine_usafacts_and_jhu)
        return [
            # 1 0
            uf(geo), None, uf(geo), None,
            # 0 1
            None, jhu(geo),
            # 1 1
            uf(geo), jhu(geo), uf(geo), jhu(geo),
            # 0 0
            None, None
        ]

    geos = ["state", "county", "msa", "nation", "hhs"]
    outputs = [df for g in geos for df in make_mock(g)]
    mock_covidcast_signal.side_effect = outputs[:]

    date_range = [date.today(), date.today()]

    calls = 0
    for geo in geos:
        for config, call_size, expected_size in [
                ("1 0", 4, 1),
                ("0 1", 2, 0),
                ("1 1", 4, 1 if geo in ["nation", "hhs"] else 2),
                ("0 0", 2, 0)
        ]:
            df = combine_usafacts_and_jhu("", geo, date_range, fetcher=mock_covidcast_signal)
            assert df.size == expected_size * len(COLUMN_MAPPING), f"""
Wrong number of rows in combined data frame for the number of available signals.

input for {geo} {config}:
{outputs[calls]}
{outputs[calls + 1]}

output:
{df}

expected rows: {expected_size}
"""
            calls += call_size
            date_count[0] += 1

@patch("covidcast.covidcast.signal")
def test_multiple_issues(mock_covidcast_signal):
    """Verify that only the most recent issue is retained."""
    mock_covidcast_signal.side_effect = [
        pd.DataFrame({
            "geo_value": ["01000", "01000"],
            "value": [1, 10],
            "timestamp": [20200101, 20200101],
            "issue": [20200102, 20200104]
        }),
        None
    ] * 2
    result = combine_usafacts_and_jhu("confirmed_incidence_num", "county", date_range=(0, 1), fetcher=mock_covidcast_signal)
    pd.testing.assert_frame_equal(
        result,
        pd.DataFrame(
            {
                "geo_id": ["01000"],
                "val": [10],
                "timestamp": [20200101],
                "issue": [20200104]
            },
            index=[1]
        )
    )

def test_compute_special_geo_dfs():
    test_df = pd.DataFrame({"geo_id": ["01000", "01001"],
                            "val": [50, 100],
                            "timestamp": [20200101, 20200101]},)
    pd.testing.assert_frame_equal(
        compute_special_geo_dfs(test_df, "_prop", "nation"),
        pd.DataFrame({"timestamp": [20200101],
                      "geo_id": ["us"],
                      "val": [150/4903185*100000]})
    )
    pd.testing.assert_frame_equal(
        compute_special_geo_dfs(test_df, "_num", "nation"),
        pd.DataFrame({"timestamp": [20200101],
                      "geo_id": ["us"],
                      "val": [150]})
    )

@patch("covidcast.covidcast.signal")
def test_get_updated_dates(mock_covidcast_signal):
    mock_covidcast_signal.side_effect = [
        pd.DataFrame({"geo_value": ["01000", "01001"],
                      "value": [50, 100],
                      "timestamp": [20200101, 20200103]}),
        pd.DataFrame({"geo_value": ["72001", "01001"],
                      "value": [200, 100],
                      "timestamp": [20200101, 20200101]})
    ]
    updated_dates = get_updated_dates(
        "confirmed_incidence_num",
        "nation",
        date_range=(0, 1),
        fetcher=mock_covidcast_signal)
    assert np.allclose(updated_dates, np.array([20200101, 20200103]))

@patch("covidcast.covidcast.signal")
def test_combine_usafacts_and_jhu_special_geos(mock_covidcast_signal):
    mock_covidcast_signal.side_effect = [
        pd.DataFrame({"geo_value": ["01000", "01001"],
                      "value": [50, 100],
                      "timestamp": [20200101, 20200101]}),
        pd.DataFrame({"geo_value": ["72001", "01001"],
                      "value": [200, 100],
                      "timestamp": [20200101, 20200101]}),
    ] * 6 # each call to combine_usafacts_and_jhu makes (2 + 2 * len(unique_timestamps)) = 12 calls to the fetcher

    pd.testing.assert_frame_equal(
        combine_usafacts_and_jhu("confirmed_incidence_num", "nation", date_range=(0, 1), fetcher=mock_covidcast_signal),
        pd.DataFrame({"timestamp": [20200101],
                      "geo_id": ["us"],
                      "val": [50 + 100 + 200],
                      "se": [None],
                      "sample_size": [None]})
    )
    pd.testing.assert_frame_equal(
        combine_usafacts_and_jhu("confirmed_incidence_prop", "nation", date_range=(0, 1), fetcher=mock_covidcast_signal),
        pd.DataFrame({"timestamp": [20200101],
                      "geo_id": ["us"],
                      "val": [(50 + 100 + 200) / (4903185 + 3723066) * 100000],
                      "se": [None],
                      "sample_size": [None]})
    )
    pd.testing.assert_frame_equal(
        combine_usafacts_and_jhu("confirmed_incidence_num", "county", date_range=(0, 1), fetcher=mock_covidcast_signal),
        pd.DataFrame({"geo_id": ["01000", "01001", "72001"],
                      "val": [50, 100, 200],
                      "timestamp": [20200101, 20200101, 20200101]},
                     index=[0, 1, 0])
    )

@patch("covidcast.covidcast.signal")
def test_no_nation_jhu(mock_covidcast_signal):
    """
    If we get JHU data that extends farther into the future than USAFacts data, trim it off.
    """
    cvc_columns = "time_value geo_value value stderr sample_size".split()
    mock_covidcast_signal.side_effect = [
        pd.DataFrame({"geo_value": ["01000"],
                      "value": [50],
                      "timestamp": [20200101]},),
        pd.DataFrame({"geo_value": ["72001", "72001"],
                      "value": [1, 1],
                      "timestamp": [20200101, 20200102]}),
        pd.DataFrame({"geo_value": ["01000"],
                      "value": [50],
                      "timestamp": [20200101]},),
        pd.DataFrame({"geo_value": ["72001"],
                      "value": [1],
                      "timestamp": [20200101]})
    ]
    result = combine_usafacts_and_jhu("_num", "nation", date_range=(0, 1), fetcher=mock_covidcast_signal)

    assert mock_covidcast_signal.call_args_list[-1] == call(
        "jhu-csse",
        "_num",
        20200101,
        20200101,
        "county"
    )
    pd.testing.assert_frame_equal(
        result,
        pd.DataFrame({"timestamp":[20200101],
                      "geo_id":["us"],
                      "val":[51],
                      "se": [None],
                      "sample_size": [None]},)
    )


def test_add_nancodes():
    df = pd.DataFrame({"geo_id": ["01000", "01001", "01001"],
                      "val": [50, 100, None],
                      "timestamp": [20200101, 20200101, 20200101]})
    expected_df = pd.DataFrame({"geo_id": ["01000", "01001", "01001"],
                      "val": [50, 100, None],
                      "timestamp": [20200101, 20200101, 20200101],
                      "missing_val": [Nans.NOT_MISSING, Nans.NOT_MISSING, Nans.UNKNOWN],
                      "missing_se": [Nans.NOT_APPLICABLE] * 3,
                      "missing_sample_size": [Nans.NOT_APPLICABLE] * 3
                      })
    df = add_nancodes(df)
    pd.testing.assert_frame_equal(df, expected_df)


if __name__ == '__main__':
    unittest.main()
