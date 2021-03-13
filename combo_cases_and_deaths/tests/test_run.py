"""Tests for running combo cases and deaths indicator."""
from datetime import date
from itertools import product
import unittest
from unittest.mock import patch
import pandas as pd

from delphi_combo_cases_and_deaths.run import (
    extend_raw_date_range,
    sensor_signal,
    combine_usafacts_and_jhu,
    compute_special_geo_dfs,
    COLUMN_MAPPING)
from delphi_combo_cases_and_deaths.constants import METRICS, SMOOTH_TYPES, SENSORS


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
    def jhu(geo, c=[0]):
        c[0] += 1
        return pd.DataFrame(
            [(date.fromordinal(c[0]),"pr" if geo == "state" else "72000",1,1,1)],
            columns="time_value geo_value value stderr sample_size".split())
    def uf(geo, c=[0]):
        c[0] += 1
        return pd.DataFrame(
            [(date.fromordinal(c[0]),"ny" if geo == "state" else "36000",1,1,1)],
            columns="time_value geo_value value stderr sample_size".split())
    def make_mock(geo):
        return [
            # 1 0
            uf(geo), None,
            # 0 1
            None, jhu(geo),
            # 1 1
            uf(geo), jhu(geo),
            # 0 0
            None, None
        ]

    geos = ["state", "county", "msa", "nation", "hhs"]
    outputs = [df for g in geos for df in make_mock(g)]
    mock_covidcast_signal.side_effect = outputs[:]
    
    date_range = [date.today(), date.today()]

    calls = -1
    for geo in geos:
        for config, expected_size in [
                ("1 0", 1),
                ("0 1", 0 if geo == "msa" else 1),
                ("1 1", 1 if geo in ["msa", "nation", "hhs"] else 2),
                ("0 0", 0)
        ]:
            calls += 1
            df = combine_usafacts_and_jhu("", geo, date_range, fetcher=mock_covidcast_signal)
            assert df.size == expected_size * len(COLUMN_MAPPING), f"""
Wrong number of rows in combined data frame for the number of available signals.

input for {geo} {config}:
{outputs[2*calls]}
{outputs[2*calls + 1]}

output:
{df}

expected rows: {expected_size}
"""


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
    ]
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
def test_combine_usafacts_and_jhu_special_geos(mock_covidcast_signal):
    mock_covidcast_signal.side_effect = [
        pd.DataFrame({"geo_value": ["01000", "01001"],
                      "value": [50, 100],
                      "timestamp": [20200101, 20200101]}),
        pd.DataFrame({"geo_value": ["72001", "01001"],
                      "value": [200, 100],
                      "timestamp": [20200101, 20200101]}),
    ] * 3
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


if __name__ == '__main__':
    unittest.main()
