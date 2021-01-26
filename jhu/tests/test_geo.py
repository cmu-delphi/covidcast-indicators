import pytest

from os.path import join

import numpy as np
import pandas as pd
from delphi_jhu.geo import geo_map, INCIDENCE_BASE
from delphi_utils import GeoMapper


class TestGeoMap:
    def test_incorrect_geo(self, jhu_confirmed_test_data):
        df = jhu_confirmed_test_data

        with pytest.raises(ValueError):
            geo_map(df, "département")

    def test_fips(self, jhu_confirmed_test_data):
        test_df = jhu_confirmed_test_data
        new_df = geo_map(test_df, "county")

        # Test the same fips and timestamps are present
        assert new_df["geo_id"].eq(test_df["fips"]).all()
        assert new_df["timestamp"].eq(test_df["timestamp"]).all()

        new_df = new_df.set_index(["geo_id", "timestamp"])
        test_df = test_df.set_index(["fips", "timestamp"])
        expected_incidence = test_df["new_counts"] / test_df["population"] * INCIDENCE_BASE
        expected_cumulative_prop = test_df["cumulative_counts"] / test_df["population"] * INCIDENCE_BASE

        # Manually calculate the proportional signals in Alabama and verify equality
        assert new_df["incidence"].eq(expected_incidence).all()
        assert new_df["cumulative_prop"].eq(expected_cumulative_prop.values).all()
        # Make sure the prop signals don't have inf values
        assert not new_df["incidence"].eq(np.inf).any()
        assert not new_df["cumulative_prop"].eq(np.inf).any()

    def test_state(self, jhu_confirmed_test_data):
        df = jhu_confirmed_test_data
        new_df = geo_map(df, "state")

        gmpr = GeoMapper()
        test_df = gmpr.replace_geocode(df, "fips", "state_id", date_col="timestamp", new_col="state")

        # Test the same states and timestamps are present
        assert new_df["geo_id"].eq(test_df["state"]).all()
        assert new_df["timestamp"].eq(test_df["timestamp"]).all()

        new_df = new_df.set_index(["geo_id", "timestamp"])
        test_df = test_df.set_index(["state", "timestamp"])

        # Get the Alabama state population total in a different way
        summed_population = df.set_index("fips").filter(regex="01\d{2}[1-9]", axis=0).groupby("fips").first()["population"].sum()
        mega_fips_record = df.set_index(["fips", "timestamp"]).loc[("01000", "2020-09-15"), "population"].sum()
        # Compare with the county megaFIPS record
        assert summed_population == mega_fips_record
        # Compare with the population in the transformed df
        assert new_df.loc["al"]["population"].eq(summed_population).all()
        # Make sure diffs and cumulative are equal
        assert new_df["new_counts"].eq(test_df["new_counts"]).all()
        assert new_df["cumulative_counts"].eq(test_df["cumulative_counts"]).all()
        # Manually calculate the proportional signals in Alabama and verify equality
        expected_incidence = test_df.loc["al"]["new_counts"] / summed_population * INCIDENCE_BASE
        expected_cumulative_prop = test_df.loc["al"]["cumulative_counts"] / summed_population * INCIDENCE_BASE
        assert new_df.loc["al", "incidence"].eq(expected_incidence).all()
        assert new_df.loc["al", "cumulative_prop"].eq(expected_cumulative_prop).all()
        # Make sure the prop signals don't have inf values
        assert not new_df["incidence"].eq(np.inf).any()
        assert not new_df["cumulative_prop"].eq(np.inf).any()

    def test_other_geos(self, jhu_confirmed_test_data):
        for geo in ["msa", "hrr", "hhs", "nation"]:
            test_df = jhu_confirmed_test_data
            new_df = geo_map(test_df, geo)
            gmpr = GeoMapper()
            test_df = gmpr.replace_geocode(test_df, "fips", geo, date_col="timestamp")

            new_df = new_df.set_index(["geo_id", "timestamp"]).sort_index()
            test_df = test_df.set_index([geo, "timestamp"]).sort_index()

            # Check that the non-proportional columns are identical
            assert new_df.eq(test_df)[["new_counts", "population", "cumulative_counts"]].all().all()
            # Check that the proportional signals are identical
            exp_incidence = test_df["new_counts"] / test_df["population"]  * INCIDENCE_BASE
            expected_cumulative_prop = test_df["cumulative_counts"] / test_df["population"]  * INCIDENCE_BASE
            assert new_df["incidence"].eq(exp_incidence).all()
            assert new_df["cumulative_prop"].eq(expected_cumulative_prop).all()
            # Make sure the prop signals don't have inf values
            assert not new_df["incidence"].eq(np.inf).any()
            assert not new_df["cumulative_prop"].eq(np.inf).any()
