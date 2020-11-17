import pytest

from os.path import join

import numpy as np
import pandas as pd

from delphi_google_symptoms.geo import geo_map

class TestGeo:
    def test_fips(self):
        df = pd.DataFrame(
            {
                "geo_id": ["53003", "48027", "50103"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15"],
                "Anosmia": [10, 15, 2],
                "Ageusia": [100, 20, 45],
                "sum_anosmia_ageusia": [110, 35, 47],
            }
        )
        new_df = geo_map(df, "county")
        
        assert set(new_df.keys()) == set(df.keys())
        assert (new_df["Anosmia"] == df["Anosmia"]).all()
        assert (new_df["Ageusia"] == df["Ageusia"]).all()
        assert (new_df["sum_anosmia_ageusia"] == df["sum_anosmia_ageusia"]).all()
        
    def test_hrr(self):
        df = pd.DataFrame(
            {
                "geo_id": ["01001", "01009", "01007"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15"],
                "Anosmia": [10, 15, 2],
                "Ageusia": [100, 20, 45],
                "sum_anosmia_ageusia": [110, 35, 47],
            }
        )
        new_df = geo_map(df, "hrr").dropna()
        
        assert set(new_df.keys()) == set(df.keys())
        assert set(new_df["geo_id"]) == set(["1", "5", "7", "9"])
        assert new_df["Anosmia"].values == pytest.approx([0.39030655604059333, 
                                                          0.014572815050225169,
                                                          1.1509470322941868,
                                                          0.08525105356979307])
        assert new_df["Ageusia"].values == pytest.approx([0.7973533171562179,
                                                   0.019430420066966894,
                                                   11.509470322941867,
                                                   1.918148705320344])
        assert new_df["sum_anosmia_ageusia"].values == pytest.approx(
                new_df["Anosmia"].values + new_df["Ageusia"])
        
    def test_msa(self):
        df = pd.DataFrame(
            {
                "geo_id": ["01001", "01009", "01007"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15"],
                "Anosmia": [10, 15, 2],
                "Ageusia": [100, 20, 45],
                "sum_anosmia_ageusia": [110, 35, 47],
            }
        )
        new_df = geo_map(df, "msa").dropna()
        
        assert set(new_df.keys()) == set(df.keys())
        assert set(new_df["geo_id"]) == set(["13820", "33860"])
        assert new_df["Anosmia"].values == pytest.approx([0.8365267072315176,
                                                          1.4966647914490074])
        assert new_df["Ageusia"].values == pytest.approx([1.9847583762443426,
                                                          14.966647914490075])
        assert new_df["sum_anosmia_ageusia"].values == pytest.approx(
                new_df["Anosmia"].values + new_df["Ageusia"])
