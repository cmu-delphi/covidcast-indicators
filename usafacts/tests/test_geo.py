import pytest

import pandas as pd
import numpy as np
from delphi_utils.geomap import GeoMapper
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
            }
        )
        new_df = geo_map(df, "county", SENSOR)
        gmpr = GeoMapper()
        df = gmpr.add_population_column(df, "fips")
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
            }
        )

        state_df = geo_map(df, "state", SENSOR)
        pd.testing.assert_frame_equal(
            state_df,
            pd.DataFrame({
                "geo_id": ["az", "ma"],
                "timestamp": ["2020-02-15"]*2,
                "new_counts": [27, 14],
                "cumulative_counts": [165, 61],
                "population": [236646., 521202.],
                "incidence": [27 / 236646 * 100000, 14 / 521202 * 100000],
                "cumulative_prop": [165 / 236646 * 100000, 61 / 521202 * 100000]
            })
        )

        hhs_df = geo_map(df, "hhs", SENSOR)
        pd.testing.assert_frame_equal(
            hhs_df,
            pd.DataFrame({
                "geo_id": ["1", "9"],
                "timestamp": ["2020-02-15"]*2,
                "new_counts": [14, 27],
                "cumulative_counts": [61, 165],
                "population": [521202., 236646.],
                "incidence": [14 / 521202 * 100000, 27 / 236646 * 100000],
                "cumulative_prop": [61 / 521202 * 100000, 165 / 236646 * 100000]
            })
        )

        nation_df = geo_map(df, "nation", SENSOR)
        pd.testing.assert_frame_equal(
            nation_df,
            pd.DataFrame({
                "geo_id": ["us"],
                "timestamp": ["2020-02-15"],
                "new_counts": [41],
                "cumulative_counts": [226],
                "population": [521202.0 + 236646],
                "incidence": [41 / (521202 + 236646) * 100000],
                "cumulative_prop": [226 / (521202 + 236646) * 100000]
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
            }
        )
        hrr_df = geo_map(df, "hrr", SENSOR)
        msa_df = geo_map(df, "msa", SENSOR)
        assert msa_df.shape == (2, 7)
        gmpr = GeoMapper()
        df = gmpr.add_population_column(df, "fips")
        assert np.isclose(hrr_df.new_counts.sum(), df.new_counts.sum())
        assert np.isclose(hrr_df.population.sum(), df.population.sum())
        assert hrr_df.shape == (5, 7)
