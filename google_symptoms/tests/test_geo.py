import pytest

from os.path import join

import numpy as np
import pandas as pd

from delphi_google_symptoms.geo import geo_map
from delphi_google_symptoms.constants import METRICS, COMBINED_METRIC

class TestGeo:
    def test_fips(self):
        df = pd.DataFrame(
            {
                "geo_id": ["53003", "48027", "50103"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15"],
                METRICS[0]: [10, 15, 2],
                METRICS[1]: [100, 20, 45],
                COMBINED_METRIC: [110, 35, 47],
            }
        )
        new_df = geo_map(df, "county")
        
        assert set(new_df.keys()) == set(df.keys())
        assert (new_df[METRICS[0]] == df[METRICS[0]]).all()
        assert (new_df[METRICS[1]] == df[METRICS[1]]).all()
        assert (new_df[COMBINED_METRIC] == df[COMBINED_METRIC]).all()
        
    def test_hrr(self):
        df = pd.DataFrame(
            {
                "geo_id": ["01001", "01009", "01007"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15"],
                METRICS[0]: [10, 15, 2],
                METRICS[1]: [100, 20, 45],
                COMBINED_METRIC: [110, 35, 47],
            }
        )
        new_df = geo_map(df, "hrr").dropna()
        
        assert set(new_df.keys()) == set(df.keys())
        assert set(new_df["geo_id"]) == set(["1", "5", "7", "9"])
        assert new_df[METRICS[0]].values == pytest.approx([0.39030655604059333, 
                                                          0.014572815050225169,
                                                          1.1509470322941868,
                                                          0.08525105356979307])
        assert new_df[METRICS[1]].values == pytest.approx([0.7973533171562179,
                                                   0.019430420066966894,
                                                   11.509470322941867,
                                                   1.918148705320344])
        assert new_df[COMBINED_METRIC].values == pytest.approx(
                list(new_df[METRICS[0]].values + new_df[METRICS[1]]))
        
    def test_msa(self):
        df = pd.DataFrame(
            {
                "geo_id": ["01001", "01009", "01007"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15"],
                METRICS[0]: [10, 15, 2],
                METRICS[1]: [100, 20, 45],
                COMBINED_METRIC: [110, 35, 47],
            }
        )
        new_df = geo_map(df, "msa").dropna()
        
        assert set(new_df.keys()) == set(df.keys())
        assert set(new_df["geo_id"]) == set(["13820", "33860"])
        assert new_df[METRICS[0]].values == pytest.approx([0.8365267072315176,
                                                          1.4966647914490074])
        assert new_df[METRICS[1]].values == pytest.approx([1.9847583762443426,
                                                          14.966647914490075])
        assert new_df[COMBINED_METRIC].values == pytest.approx(
                list(new_df[METRICS[0]].values + new_df[METRICS[1]]))
