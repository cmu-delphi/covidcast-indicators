from os.path import join

import pytest

import numpy as np
import pandas as pd
from delphi_usafacts.geo import disburse, geo_map

MAP_DF = pd.read_csv(
    join("..", "static", "fips_prop_pop.csv"),
    dtype={"fips": int}
)

SENSOR = "new_counts"

class TestDisburse:
    """Tests for the `geo.disburse()` function."""
    def test_even(self):
        """Tests that values are disbursed evenly across recipients."""
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
    """Tests for `geo.geo_map()`."""
    def test_incorrect_geo(self):
        """Tests that an invalid resolution raises an error."""
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
            geo_map(df, "département", MAP_DF, SENSOR)

    def test_county(self):
        """Tests that values are correctly aggregated at the county level."""
        df = pd.DataFrame(
            {
                "fips": ["53003", "48027", "50103"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15"],
                "new_counts": [10, 15, 2],
                "cumulative_counts": [100, 20, 45],
                "population": [100, 2100, 300],
            }
        )

        new_df = geo_map(df, "county", MAP_DF, SENSOR)

        exp_incidence = df["new_counts"] / df["population"] * 100000
        exp_cprop = df["cumulative_counts"] / df["population"] * 100000

        assert set(new_df["geo_id"].values) == set(df["fips"].values)
        assert set(new_df["timestamp"].values) == set(df["timestamp"].values)
        assert set(new_df["incidence"].values) == set(exp_incidence.values)
        assert set(new_df["cumulative_prop"].values) == set(exp_cprop.values)

    def test_state(self):
        """Tests that values are correctly aggregated at the state level."""
        df = pd.DataFrame(
            {
                "fips": ["04001", "04003", "04009", "25023", "25000"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15", "2020-02-15", "2020-02-15"],
                "new_counts": [10, 15, 2, 13, 0],
                "cumulative_counts": [100, 20, 45, 60, 0],
                "population": [100, 2100, 300, 25, 25],
            }
        )

        new_df = geo_map(df, "state", MAP_DF, SENSOR)

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
        """Tests that values are correctly aggregated at the HRR level."""
        df = pd.DataFrame(
            {
                "fips": ["13009", "13017", "13021", "09015"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15", "2020-02-15"],
                "new_counts": [10, 15, 2, 13],
                "cumulative_counts": [100, 20, 45, 60],
                "population": [100, 2100, 300, 25],
            }
        )

        new_df = geo_map(df, "hrr", MAP_DF, SENSOR)

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
        """Tests that values are correctly aggregated at the MSA level."""
        df = pd.DataFrame(
            {
                "fips": ["13009", "13017", "13021", "09015"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15", "2020-02-15"],
                "new_counts": [10, 15, 2, 13],
                "cumulative_counts": [100, 20, 45, 60],
                "population": [100, 2100, 300, 25],
            }
        )

        new_df = geo_map(df, "msa", MAP_DF, SENSOR)

        exp_incidence = np.array([2, 13]) / np.array([300, 25]) * 100000
        exp_cprop = np.array([45, 60]) / np.array([300, 25]) * 100000

        assert (new_df["geo_id"].values == [31420, 49340]).all()
        assert (new_df["timestamp"].values == ["2020-02-15", "2020-02-15"]).all()
        assert new_df["new_counts"].values == pytest.approx([2.0, 13.0])
        assert new_df["cumulative_counts"].values == pytest.approx([45, 60])
        assert new_df["population"].values == pytest.approx([300, 25])
        assert new_df["incidence"].values == pytest.approx(exp_incidence)
        assert new_df["cumulative_prop"].values == pytest.approx(exp_cprop)
