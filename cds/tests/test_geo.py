import pytest

from os.path import join

import numpy as np
import pandas as pd
from delphi_cds.geo import fips_to_state, disburse, geo_map

MAP_DF = pd.read_csv(
    join("..", "static", "fips_prop_pop.csv"),
    dtype={"fips": int}
)


class TestFipsToState:

    def test_normal(self):

        assert fips_to_state("53003") == "wa"
        assert fips_to_state("48027") == "tx"
        assert fips_to_state("12003") == "fl"
        assert fips_to_state("50103") == "vt"
        assert fips_to_state("15003") == "hi"
    
    def test_mega(self):
        
        assert fips_to_state("01000") == "al"
        assert fips_to_state("13000") == "ga"
        assert fips_to_state("44000") == "ri"
        assert fips_to_state("12000") == "fl"


class TestDisburse:
    def test_even(self):

        df = pd.DataFrame(
            {
                "fips": ["51093", "51175", "51620"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15"],
                "new_counts": [3, 2, 2],
                "cumulative_counts": [13, 12, 12],
                "population": [100, 2100, 300],
            }
        ).sort_values(["fips", "timestamp"])

        new_df = disburse(df, "51620", ["51093", "51175"])

        assert new_df["new_counts"].values == pytest.approx([4, 3, 2])
        assert new_df["cumulative_counts"].values == pytest.approx([19, 18, 12])


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
            geo_map(df, "département", MAP_DF)

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
        
        new_df = geo_map(df, "county", MAP_DF)
        
        df = df.sort_values("fips")

        exp_incidence = df["new_counts"] / df["population"] * 100000
        exp_cprop = df["cumulative_counts"] / df["population"] * 100000
        
        assert (new_df["geo_id"].values == ['48027', '50103', '53003']).all()
        assert (new_df["timestamp"].values == df["timestamp"].values).all()
        assert (new_df["incidence"].values == exp_incidence.values).all()
        assert (new_df["cumulative_prop"].values == exp_cprop.values).all()

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

        new_df = geo_map(df, "state", MAP_DF)
        
        df = df.sort_values("fips")
        exp_incidence = np.array([27, 13]) / np.array([2500, 25]) * 100000
        exp_cprop = np.array([165, 60]) / np.array([2500, 25]) * 100000

        assert (new_df["geo_id"].values == ["az", "ma"]).all()
        assert (new_df["timestamp"].values == ["2020-02-15", "2020-02-15"]).all()
        assert (new_df["new_counts"].values == [27, 13]).all()
        assert (new_df["cumulative_counts"].values == [165, 60]).all()
        assert (new_df["population"].values == [2500, 25]).all()
        assert (new_df["incidence"].values == exp_incidence).all()
        assert (new_df["cumulative_prop"].values == exp_cprop).all()

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
        
        new_df = geo_map(df, "hrr", MAP_DF)

        exp_incidence = np.array([13, 27]) / np.array([25, 2500]) * 100000
        exp_cprop = np.array([60, 165]) / np.array([25, 2500]) * 100000

        assert (new_df["geo_id"].values == [110, 147]).all()
        assert (new_df["timestamp"].values == ["2020-02-15", "2020-02-15"]).all()
        assert new_df["new_counts"].values == pytest.approx([13.0, 27.0])
        assert new_df["cumulative_counts"].values == pytest.approx([60, 165])
        assert new_df["population"].values == pytest.approx([25, 2500])
        assert new_df["incidence"].values == pytest.approx(exp_incidence)
        assert new_df["cumulative_prop"].values == pytest.approx(exp_cprop)

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

        new_df = geo_map(df, "msa", MAP_DF)

        exp_incidence = np.array([2, 13]) / np.array([300, 25]) * 100000
        exp_cprop = np.array([45, 60]) / np.array([300, 25]) * 100000

        assert (new_df["geo_id"].values == [31420, 49340]).all()
        assert (new_df["timestamp"].values == ["2020-02-15", "2020-02-15"]).all()
        assert new_df["new_counts"].values == pytest.approx([2.0, 13.0])
        assert new_df["cumulative_counts"].values == pytest.approx([45, 60])
        assert new_df["population"].values == pytest.approx([300, 25])
        assert new_df["incidence"].values == pytest.approx(exp_incidence)
        assert new_df["cumulative_prop"].values == pytest.approx(exp_cprop)
