import pytest

from os.path import join

import numpy as np
import pandas as pd
from delphi_jhu.geo import geo_map


class TestGeoMap:
    def test_incorrect_geo(self):
        df = pd.DataFrame(
            {
                "fips": ["53003", "48027", "50103"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15"],
                "new_counts": [10, 15, 2],
                "cumulative_counts": [100, 20, 45],
                "population": [100, 2100, 300],
            }
        )

        with pytest.raises(ValueError):
            geo_map(df, "département")

    def test_county(self):
        df = pd.DataFrame(
            {
                "fips": ["53003", "48027", "50103"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15"],
                "new_counts": [10, 15, 2],
                "cumulative_counts": [100, 20, 45],
                "population": [100, 2100, 300],
            }
        )

        df_mega = pd.DataFrame(
            {
                "fips": ["13000", "01000"],
                "timestamp": ["2020-02-15", "2020-02-15"],
                "new_counts": [8, 2],
                "cumulative_counts": [80, 12],
                "population": [np.nan, np.nan],
            }
        )

        df = df.append(df_mega)

        new_df = geo_map(df, "county")

        exp_incidence = df["new_counts"] / df["population"] * 100000
        exp_cprop = df["cumulative_counts"] / df["population"] * 100000

        assert set(new_df["geo_id"].values) == set(['01000', '13000', '48027', '50103', '53003'])
        assert set(new_df["timestamp"].values) == set(df["timestamp"].values)
        assert new_df["incidence"].isin(exp_incidence.values).all()
        assert new_df["cumulative_prop"].isin(exp_cprop.values).all()

    def test_state(self):
        df = pd.DataFrame(
            {
                "fips": ["04001", "04003", "04009", "25023"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15", "2020-02-15"],
                "new_counts": [10, 15, 2, 13],
                "cumulative_counts": [100, 20, 45, 60],
                "population": [100, 2100, 300, 25],
            }
        )

        df_mega = pd.DataFrame(
            {
                "fips": ["13000", "01000", "04000", "25000"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15", "2020-02-15"],
                "new_counts": [8, 2, 5, 10],
                "cumulative_counts": [80, 12, 30, 100],
                "population": [np.nan, np.nan, np.nan, np.nan],
            }
        )

        df = df.append(df_mega)

        new_df = geo_map(df, "state")

        exp_incidence = np.array([27 + 5, 13 + 10]) / np.array([2500, 25]) * 100000
        exp_cprop = np.array([165 + 30, 60 + 100]) / np.array([2500, 25]) * 100000

        assert set(new_df["geo_id"].values) == set(["AZ", "MA", "AL", "GA"])
        assert set(new_df["timestamp"].values) == set(["2020-02-15"])
        assert set(new_df["new_counts"].values) == set([32, 23, 2, 8])
        assert set(new_df["cumulative_counts"].values) == set([195, 160, 12, 80])
        assert set(new_df["population"].values) == set([2500, 25, 0])
        assert set(new_df["incidence"].values) - set(exp_incidence) == set([np.Inf])
        assert set(new_df["cumulative_prop"].values) - set(exp_cprop) == set([np.Inf])

    def test_hrr(self):
        df = pd.DataFrame(
            {
                "fips": ["13009", "13017", "13021", "09015"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15", "2020-02-15"],
                "new_counts": [10, 15, 2, 13],
                "cumulative_counts": [100, 20, 45, 60],
                "population": [100, 2100, 300, 25],
            }
        )

        # df_mega = pd.DataFrame(
        #     {
        #         "fips": ["90013", "90001"],
        #         "timestamp": ["2020-02-15", "2020-02-15"],
        #         "new_counts": [8, 2],
        #         "cumulative_counts": [80, 12],
        #         "population": [np.nan, np.nan],
        #     }
        # )

        # df = df.append(df_mega)

        new_df = geo_map(df, "hrr")

        exp_incidence = np.array([13, 27]) / np.array([25, 2500]) * 100000
        exp_cprop = np.array([60, 165]) / np.array([25, 2500]) * 100000

        assert new_df["geo_id"].isin([110, 123, 140, 145, 147]).all()
        assert new_df["timestamp"].isin(["2020-02-15"]).all()

    def test_msa(self):
        df = pd.DataFrame(
            {
                "fips": ["13009", "13017", "13021", "09015"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15", "2020-02-15"],
                "new_counts": [10, 15, 2, 13],
                "cumulative_counts": [100, 20, 45, 60],
                "population": [100, 2100, 300, 25],
            }
        )

        # df_mega = pd.DataFrame(
        #     {
        #         "fips": ["90013", "90001"],
        #         "timestamp": ["2020-02-15", "2020-02-15"],
        #         "new_counts": [8, 2],
        #         "cumulative_counts": [80, 12],
        #         "population": [np.nan, np.nan],
        #     }
        # )

        # df = df.append(df_mega)

        new_df = geo_map(df, "msa")

        assert new_df["geo_id"].isin([31420, 49340]).all()
        assert new_df["timestamp"].isin(["2020-02-15"]).all()
