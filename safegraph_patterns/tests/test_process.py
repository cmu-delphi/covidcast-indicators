import pytest

from os import listdir
from os.path import join

import numpy as np
import pandas as pd
from delphi_safegraph.process import (
        construct_signals,
        aggregate,
    )
from delphi_safegraph.run import METRICSS

metric_names, naics_codes, _ = (list(x) for x in zip(*METRICS))

map_df = pd.read_csv(
        join("../static", "mapping_and_pop_info.csv"), dtype={"fips": int}
    ).rename({
            "fips":"county", "hrrnum":"hrr",
            "cbsa_id":"msa", "state_id":"state"
    }, axis = 1)
brand_df = pd.read_csv(
                join("../static", f"brand_info/brand_info_202004.csv")
        )

class TestProcess:
    def test_construct_signals_present(self):

        df = pd.read_csv('sample_raw_data.csv',
                         parse_dates=["date_range_start", "date_range_end"])
        df = construct_signals(df, metric_names, naics_codes, brand_df)
        assert set(["timestamp", "zip", "bars_visit_num", 
                    "restaurants_visit_num"]) == set(df.columns)
        assert df["timestamp"].unique().shape[0] == 7

    def test_aggregate_county(self):
    
        df = pd.read_csv('sample_filtered_data.csv', parse_dates=["timestamp"])
        df_export = aggregate(df, metric_names, "county", map_df)

        assert np.all(df_export[f'{metric_names[0]}_num'].dropna().values > 0)
        assert np.all(df_export[f'{metric_names[1]}_num'].dropna().values >= 0)
        assert set(["timestamp", "geo_id", "bars_visit_num", "bars_visit_prop", 
                    "restaurants_visit_num", "restaurants_visit_prop", 
                    "population"]) == set(df_export.columns)

    def test_aggregate_state(self):
    
        df = pd.read_csv('sample_filtered_data.csv', parse_dates=["timestamp"])
        df_export = aggregate(df, metric_names, "state", map_df)

        assert np.all(df_export[f'{metric_names[0]}_num'].dropna().values > 0)
        assert np.all(df_export[f'{metric_names[1]}_num'].dropna().values >= 0)
        assert set(["timestamp", "geo_id", "bars_visit_num", "bars_visit_prop", 
                    "restaurants_visit_num", "restaurants_visit_prop", 
                    "population"]) == set(df_export.columns)

