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

    def test_state_hhs_nation(self):
        """Tests that values are correctly aggregated at the state, HHS, and nation level."""
        df = pd.DataFrame(
            {
                "fips": ["04001", "04003", "04009", "25023", "25000"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15", "2020-02-15", "2020-02-15"],
                "new_counts": [10, 15, 2, 13, 1],
                "cumulative_counts": [100, 20, 45, 60, 1],
                "population": [100, 2100, 300, 25, 25],
            }
        )

        state_df = geo_map(df, "state", SENSOR)
        pd.testing.assert_frame_equal(
            state_df,
            pd.DataFrame({
                "geo_id": ["az", "ma"],
                "timestamp": ["2020-02-15"]*2,
                "new_counts": [27.0, 14.0],
                "cumulative_counts": [165.0, 61.0],
                "population": [7278717, 6892503],
                "incidence": [27 / 7278717 * 100000, 14 / 6892503 * 100000],
                "cumulative_prop": [165 / 7278717 * 100000, 61 / 6892503 * 100000]
            })
        )

        hhs_df = geo_map(df, "hhs", SENSOR)
        pd.testing.assert_frame_equal(
            hhs_df,
            pd.DataFrame({
                "geo_id": ["1", "9"],
                "timestamp": ["2020-02-15"]*2,
                "new_counts": [14.0, 27.0],
                "cumulative_counts": [61.0, 165.0],
                "population": [6892503, 7278717],
                "incidence": [14 / 6892503 * 100000, 27 / 7278717 * 100000],
                "cumulative_prop": [61 / 6892503 * 100000, 165 / 7278717 * 100000]
            })
        )

        nation_df = geo_map(df, "nation", SENSOR)
        pd.testing.assert_frame_equal(
            nation_df,
            pd.DataFrame({
                "geo_id": ["us"],
                "timestamp": ["2020-02-15"],
                "new_counts": [41.0],
                "cumulative_counts": [226.0],
                "population": [7278717 + 6892503],
                "incidence": [41 / (7278717 + 6892503) * 100000],
                "cumulative_prop": [226 / (7278717 + 6892503) * 100000]
            })
        )

    def test_hrr_msa(self):
        """Tests that values are correctly aggregated at the HRR and MSA level."""
        df = pd.DataFrame(
            {
                "fips": ["13009", "13017", "13021", "09015"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15", "2020-02-15"],
                "new_counts": [10, 15, 2, 13],
                "cumulative_counts": [100, 20, 45, 60],
                "population": [100, 2100, 300, 25],
            }
        )
        hrr_df = geo_map(df, "hrr", SENSOR)
        pd.testing.assert_frame_equal(
            hrr_df,
            pd.DataFrame({
                "geo_id": ["110", "123", "140", "145", "147"],
                "timestamp": ["2020-02-15"]*5,
                "new_counts": [13.0, 0.111432, 0.098673, 0.0080927, 26.7818017],
                "cumulative_counts": [60.0, 0.148577, 0.131564, 0.080927, 164.638932],
                "population": [25.0, 15.600544, 13.814223, 0.080927, 2470.504306],
                "incidence": [52000.0, 714.285714, 714.285714, 10000.0, 1084.062138],
                "cumulative_prop": [240000.0, 952.380952, 952.380952, 100000.0, 6664.183163]
            })
        )

        msa_df = geo_map(df, "msa", SENSOR)
        pd.testing.assert_frame_equal(
            msa_df,
            pd.DataFrame({
                "geo_id": ["31420", "49340"],
                "timestamp": ["2020-02-15"]*2,
                "new_counts": [2, 13],
                "cumulative_counts": [45, 60],
                "population": [300, 25],
                "incidence": [666.66667, 52000.0],
                "cumulative_prop": [15000.0, 240000.0]
            })
        )

        hhs_df = geo_map(df, "hhs", SENSOR)
        pd.testing.assert_frame_equal(
            hhs_df,
            pd.DataFrame({
                "geo_id": ["1", "4"],
                "timestamp": ["2020-02-15"]*2,
                "new_counts": [13, 27],
                "cumulative_counts": [60, 165],
                "population": [25, 2500],
                "incidence": [52000.0, 1080.0],
                "cumulative_prop": [240000.0, 6600.0]
            })
        )

        nation_df = geo_map(df, "nation", SENSOR)
        pd.testing.assert_frame_equal(
            nation_df,
            pd.DataFrame({
                "geo_id": ["us"],
                "timestamp": ["2020-02-15"],
                "new_counts": [40],
                "cumulative_counts": [225],
                "population": [2525],
                "incidence": [1584.15842],
                "cumulative_prop": [8910.89109]
            })
        )
