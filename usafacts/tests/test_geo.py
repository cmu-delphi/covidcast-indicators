from os.path import join

import pytest

import numpy as np
import pandas as pd
from delphi_usafacts.geo import disburse, geo_map

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
            geo_map(df, "département", SENSOR)

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

        new_df = geo_map(df, "county", SENSOR)

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

        new_df = geo_map(df, "state", SENSOR)

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
        new_df = geo_map(df, "hrr", SENSOR)
        pd.testing.assert_frame_equal(
            new_df.round(5),
            pd.DataFrame({
                "geo_id": ["110", "123", "140", "145", "147"],
                "timestamp": ["2020-02-15"]*5,
                "new_counts": [13.0, 0.11143, 0.09867, 0.00809, 26.78180],
                "cumulative_counts": [60.0, 0.14858, 0.13156, 0.08093, 164.63893],
                "population": [25.0, 15.60054, 13.81422, 0.08093, 2470.50431],
                "incidence": [52000.0, 714.28571, 714.28571, 10000.0, 1084.06214],
                "cumulative_prop": [240000.0, 952.38095, 952.38095, 100000.0, 6664.18316]
            })
        )

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
        new_df = geo_map(df, "msa", SENSOR)
        pd.testing.assert_frame_equal(
            new_df.round(5),
            pd.DataFrame({
                "geo_id": ["31420", "49340"],
                "timestamp": ["2020-02-15"]*2,
                "new_counts": [2.0, 13.0],
                "cumulative_counts": [45.0, 60.0],
                "population": [300, 25],
                "incidence": [666.66667, 52000.0],
                "cumulative_prop": [15000.0, 240000.0]
            })
        )
