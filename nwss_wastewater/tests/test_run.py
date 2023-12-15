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
from delphi_nwss.run import generate_weights, sum_all_nan


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
    weighted
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
