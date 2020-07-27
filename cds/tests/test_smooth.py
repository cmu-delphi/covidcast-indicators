import pytest

from os import listdir
from os.path import join

import numpy as np
import pandas as pd
from delphi_cds.run import run_module

class TestSmooth:
    def test_output_files_smoothed(self, run_as_module):

        dates = [str(x) for x in range(20200704, 20200711)]

        smoothed = pd.read_csv(
            join("receiving",
                f"{dates[-1]}_state_wip_tested_7dav_cumul_num.csv")
        )

        raw = pd.concat([
            pd.read_csv(
                join("receiving",
                    f"{date}_state_wip_tested_cumulative_num.csv")
            ) for date in dates
        ])

        raw = raw.groupby('geo_id')['val'].mean()
        df = pd.merge(smoothed, raw, on='geo_id', suffixes=('_smoothed', '_raw'))
        
        assert np.allclose(df['val_smoothed'].values, df['val_raw'].values)
