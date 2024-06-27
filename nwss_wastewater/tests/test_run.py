import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from delphi_nwss.run import add_needed_columns


def test_adding_cols():
    df = pd.DataFrame({"val": [0.0, np.nan], "timestamp": np.zeros(2)})
    modified = add_needed_columns(df)
    modified
    expected_df = pd.DataFrame(
        {
            "val": [0.0, np.nan],
            "timestamp": np.zeros(2),
            "se": [np.nan, np.nan],
            "sample_size": [np.nan, np.nan],
            "missing_val": [0, 5],
            "missing_se": [1, 1],
            "missing_sample_size": [1, 1],
        }
    )
    assert_frame_equal(modified, expected_df)
