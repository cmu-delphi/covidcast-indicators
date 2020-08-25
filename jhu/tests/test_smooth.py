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
                f"{dates[-1]}_county_confirmed_7dav_cumulative_num.csv")
        )

        raw = pd.concat([
            pd.read_csv(
                join("./receiving",
                    f"{date}_county_confirmed_cumulative_num.csv")
            ) for date in dates
        ])

        raw = raw.groupby('geo_id')['val'].mean()
        df = pd.merge(smoothed, raw, on='geo_id', suffixes=('_smoothed', '_raw'))
        
        assert np.allclose(df['val_smoothed'].values, df['val_raw'].values)

# TestSmooth().test_output_files_smoothed(1)
