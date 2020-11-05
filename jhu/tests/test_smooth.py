import pytest

from os import listdir
from os.path import join

import numpy as np
import pandas as pd
from delphi_jhu.run import run_module

class TestSmooth:
    def test_output_files_smoothed(self, run_as_module):

        dates = [str(x) for x in range(20200303, 20200310)]

        smoothed = pd.read_csv(
            join("./receiving",
                f"{dates[-1]}_state_confirmed_7dav_cumulative_num.csv")
        )

        # Build a dataframe out of the individual day files
        raw = pd.concat([
            pd.read_csv(
                join("./receiving",
                    f"{date}_state_confirmed_cumulative_num.csv")
            ) for date in dates
        ])
        # Compute the mean across the time values; order doesn't matter 
        # this corresponds to the smoothed value on the last day 
        # 2020-03-10
        raw = raw.groupby('geo_id')['val'].mean()
        
        df = pd.merge(smoothed, raw, on='geo_id', suffixes=('_smoothed', '_raw'))
        assert np.allclose(df['val_smoothed'].values, df['val_raw'].values)

