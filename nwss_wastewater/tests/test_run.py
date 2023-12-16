from datetime import datetime, date
import json
from unittest.mock import patch
import tempfile
import os
import time
from datetime import datetime

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
from delphi_utils import S3ArchiveDiffer, get_structured_logger, create_export_csv, Nans

from delphi_nwss.constants import GEOS, SIGNALS
from delphi_nwss.pull import pull_nwss_data
from delphi_nwss.run import (
    generate_weights,
    sum_all_nan,
    weighted_state_sum,
    weighted_nation_sum,
)


def test_sum_all_nan():
    """Check that sum_all_nan returns NaN iff everything is a NaN"""
    no_nans = np.array([3, 5])
    assert sum_all_nan(no_nans) == 8
    partial_nan = np.array([np.nan, 3, 5])
    assert np.isclose(sum_all_nan([np.nan, 3, 5]), 8)

    oops_all_nans = np.array([np.nan, np.nan])
    assert np.isnan(oops_all_nans).all()


def test_weight_generation():
    dataFrame = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, np.nan],
            "b": [5, 6, 7, 8, 9],
            "population_served": [10, 5, 8, 1, 3],
        }
    )
    weighted = generate_weights(dataFrame, column_aggregating="a")
    weighted_by_hand = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, np.nan],
            "b": [5, 6, 7, 8, 9],
            "population_served": [10, 5, 8, 1, 3],
            "relevant_pop_a": [10, 5, 8, 1, 0],
            "weighted_a": [10.0, 2 * 5.0, 3 * 8, 4.0 * 1, np.nan * 0],
        }
    )
    assert_frame_equal(weighted, weighted_by_hand)
    # operations are in-place
    assert_frame_equal(weighted, dataFrame)


def test_weighted_state_sum():
    dataFrame = pd.DataFrame(
        {
            "state": [
                "al",
                "al",
                "ca",
                "ca",
                "nd",
            ],
            "timestamp": np.zeros(5),
            "a": [1, 2, 3, 4, 12],
            "b": [5, 6, 7, np.nan, np.nan],
            "population_served": [10, 5, 8, 1, 3],
        }
    )
    weighted = generate_weights(dataFrame, column_aggregating="b")
    agg = weighted_state_sum(weighted, "state", "b")
    expected_agg = pd.DataFrame(
        {
            "timestamp": np.zeros(3),
            "geo_id": ["al", "ca", "nd"],
            "relevant_pop_b": [10 + 5, 8 + 0, 0],
            "weighted_b": [5 * 10 + 6 * 5, 7 * 8 + 0, np.nan],
            "val": [80 / 15, 56 / 8, np.nan],
        }
    )
    assert_frame_equal(agg, expected_agg)

    weighted = generate_weights(dataFrame, column_aggregating="a")
    agg_a = weighted_state_sum(weighted, "state", "a")
    expected_agg_a = pd.DataFrame(
        {
            "timestamp": np.zeros(3),
            "geo_id": ["al", "ca", "nd"],
            "relevant_pop_a": [10 + 5, 8 + 1, 3],
            "weighted_a": [1 * 10 + 2 * 5, 3 * 8 + 1 * 4, 12 * 3],
            "val": [20 / 15, 28 / 9, 36 / 3],
        }
    )
    assert_frame_equal(agg_a, expected_agg_a)


def test_weighted_nation_sum():
    dataFrame = pd.DataFrame(
        {
            "state": [
                "al",
                "al",
                "ca",
                "ca",
                "nd",
            ],
            "timestamp": np.hstack((np.zeros(3), np.ones(2))),
            "a": [1, 2, 3, 4, 12],
            "b": [5, 6, 7, np.nan, np.nan],
            "population_served": [10, 5, 8, 1, 3],
        }
    )
    weighted = generate_weights(dataFrame, column_aggregating="a")
    agg = weighted_nation_sum(weighted, "a")
    expected_agg = pd.DataFrame(
        {
            "timestamp": [0.0, 1],
            "relevant_pop_a": [10 + 5 + 8, 1 + 3],
            "weighted_a": [1 * 10 + 2 * 5 + 3 * 8, 1 * 4 + 3 * 12],
            "val": [44 / 23, 40 / 4],
            "geo_id": ["us", "us"],
        }
    )
    assert_frame_equal(agg, expected_agg)
