import pytest

from os.path import join

import numpy as np
import pandas as pd

from delphi_google_symptoms.geo import geo_map
from delphi_google_symptoms.constants import METRICS, COMBINED_METRIC
from delphi_utils.geomap import GeoMapper

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
