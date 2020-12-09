from os.path import join

import numpy as np
import pandas as pd

class TestSmooth:
    def test_output_files_smoothed(self, run_as_module):

        dates = [str(x) for x in range(20200804, 20200811)]

        smoothed = pd.read_csv(
            join("receiving",
                f"{dates[-1]}_state_anosmia_smoothed_search.csv")
        )

        raw = pd.concat([
            pd.read_csv(
                join("receiving",
                    f"{date}_state_anosmia_raw_search.csv")
            ) for date in dates
        ])

        raw = raw.groupby('geo_id')['val'].sum()/7.0
        df = pd.merge(smoothed, raw, on='geo_id', suffixes=('_smoothed', '_raw'))
        
        assert np.allclose(df['val_smoothed'].values, df['val_raw'].values)
