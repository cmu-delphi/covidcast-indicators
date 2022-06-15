from os.path import join

import numpy as np
import pandas as pd
from delphi_safegraph_patterns.process import (
        construct_signals,
        aggregate,
        INCIDENCE_BASE
    )
from delphi_safegraph_patterns.run import METRICS

metric_names, naics_codes, _ = (list(x) for x in zip(*METRICS))

brand_df = pd.read_csv(
                join("./static", "brand_info/brand_info_202004.csv")
        )

class TestProcess:
    def test_construct_signals_present(self):
        result_dfs = {}
        for i, naics_code in enumerate(naics_codes):
            metric_name = metric_names[i]
            df = pd.read_csv(f'test_data/sample_raw_data_{naics_code}.csv',
                             parse_dates=["date_range_start", "date_range_end"])
            dfs = construct_signals(df, metric_names[i])
            result_dfs[metric_name] = dfs
        assert set(["timestamp", "zip",
                    "bars_visit_v2_num"]) == set(result_dfs["bars_visit_v2"].columns)
        assert set(["timestamp", "zip", "restaurants_visit_v2_num"]) == \
                  set(result_dfs["restaurants_visit_v2"].columns)
        assert result_dfs["bars_visit_v2"]["timestamp"].unique().shape[0] == 7
        assert result_dfs["restaurants_visit_v2"]["timestamp"].unique().shape[0] == 7

    def test_aggregate_county(self):

        df = pd.read_csv('test_data/sample_filtered_data.csv', parse_dates=["timestamp"])
        df_export = aggregate(df, "bars_visit", "county")

        assert np.all(df_export["bars_visit_num"].values >= 0)
        assert np.all(df_export["bars_visit_prop"].dropna().values <= INCIDENCE_BASE)
        assert set(["timestamp", "geo_id", "bars_visit_num", "bars_visit_prop",
                    "population"]) == set(df_export.columns)

    def test_aggregate_state(self):

        df = pd.read_csv('test_data/sample_filtered_data.csv', parse_dates=["timestamp"])
        df_export = aggregate(df, "bars_visit", "state")

        assert np.all(df_export["bars_visit_num"].values >= 0)
        assert np.all(df_export["bars_visit_prop"].dropna().values <= INCIDENCE_BASE)
        assert set(["timestamp", "geo_id", "bars_visit_num", "bars_visit_prop",
                    "population"]) == set(df_export.columns)
