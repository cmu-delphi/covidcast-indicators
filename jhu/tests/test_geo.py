import pytest

import numpy as np
import pandas as pd
from delphi_jhu.geo import geo_map, add_county_pop, INCIDENCE_BASE
from delphi_utils import GeoMapper

from delphi_jhu.geo import geo_map, INCIDENCE_BASE

class TestGeoMap:
    def test_incorrect_geo(self, jhu_confirmed_test_data):
        df = jhu_confirmed_test_data

        with pytest.raises(ValueError):
            geo_map(df, "département", "cumulative_prop")

    def test_fips(self, jhu_confirmed_test_data):
        test_df = jhu_confirmed_test_data
        fips_df = geo_map(test_df, "county", "cumulative_prop")
        test_df = fips_df.loc[(fips_df.geo_id == "01001") & (fips_df.timestamp == "2020-09-15")]
        gmpr = GeoMapper()
        fips_pop = gmpr.get_crosswalk("fips", "pop")
        pop01001 = float(fips_pop.loc[fips_pop.fips == "01001", "pop"])
        expected_df = pd.DataFrame({
            "geo_id": "01001",
            "timestamp": pd.Timestamp("2020-09-15"),
            "cumulative_counts": 1463.0,
            "new_counts": 1463.0,
            "population": pop01001,
            "incidence": 1463 / pop01001 * INCIDENCE_BASE,
            "cumulative_prop": 1463 / pop01001 * INCIDENCE_BASE
            }, index=[36])
        pd.testing.assert_frame_equal(test_df, expected_df)
        # Make sure the prop signals don't have inf values
        assert not fips_df["incidence"].eq(np.inf).any()
        assert not fips_df["cumulative_prop"].eq(np.inf).any()
        # make sure no megafips reported
        assert not any(i[0].endswith("000") for i in fips_df.geo_id)

    def test_state_hhs_nation(self, jhu_confirmed_test_data):
        df = jhu_confirmed_test_data
        state_df = geo_map(df, "state", "cumulative_prop")
        test_df = state_df.loc[(state_df.geo_id == "al") & (state_df.timestamp == "2020-09-15")]
        gmpr = GeoMapper()
        state_pop = gmpr.get_crosswalk("state_id", "pop")
        al_pop = float(state_pop.loc[state_pop.state_id == "al", "pop"])
        expected_df = pd.DataFrame({
            "timestamp": pd.Timestamp("2020-09-15"),
            "geo_id": "al",
            "cumulative_counts": 140160.0,
            "new_counts": 140160.0,
            "population": al_pop,
            "incidence": 140160 / al_pop * INCIDENCE_BASE,
            "cumulative_prop": 140160 / al_pop * INCIDENCE_BASE
        }, index=[1])
        pd.testing.assert_frame_equal(test_df, expected_df)
        test_df = state_df.loc[(state_df.geo_id == "gu") & (state_df.timestamp == "2020-09-15")]
        gu_pop = float(state_pop.loc[state_pop.state_id == "gu", "pop"])
        expected_df = pd.DataFrame({
            "timestamp": pd.Timestamp("2020-09-15"),
            "geo_id": "gu",
            "cumulative_counts": 502.0,
            "new_counts": 16.0,
            "population": gu_pop,
            "incidence": 16 / gu_pop * INCIDENCE_BASE,
            "cumulative_prop": 502 / gu_pop * INCIDENCE_BASE
        }, index=[11])
        pd.testing.assert_frame_equal(test_df, expected_df)

        # Make sure the prop signals don't have inf values
        assert not state_df["incidence"].eq(np.inf).any()
        assert not state_df["cumulative_prop"].eq(np.inf).any()

        hhs_df = geo_map(df, "hhs", "cumulative_prop")
        test_df = hhs_df.loc[(hhs_df.geo_id == "1") & (hhs_df.timestamp == "2020-09-15")]
        hhs_pop = gmpr.get_crosswalk("hhs", "pop")
        pop1 = float(hhs_pop.loc[hhs_pop.hhs == "1", "pop"])
        expected_df = pd.DataFrame({
            "timestamp": pd.Timestamp("2020-09-15"),
            "geo_id": "1",
            "cumulative_counts": 218044.0,
            "new_counts": 218044.0,
            "population": pop1,
            "incidence": 218044 / pop1 * INCIDENCE_BASE,
            "cumulative_prop": 218044 / pop1 * INCIDENCE_BASE
        }, index=[0])
        pd.testing.assert_frame_equal(test_df, expected_df)
        # Make sure the prop signals don't have inf values
        assert not hhs_df["incidence"].eq(np.inf).any()
        assert not hhs_df["cumulative_prop"].eq(np.inf).any()

        nation_df = geo_map(df, "nation", "cumulative_prop")
        test_df = nation_df.loc[(nation_df.geo_id == "us") & (nation_df.timestamp == "2020-09-15")]
        fips_pop = gmpr.replace_geocode(add_county_pop(df, gmpr), "fips", "nation")
        nation_pop = float(fips_pop.loc[(fips_pop.nation == "us") & (fips_pop.timestamp == "2020-09-15"), "population"])
        expected_df = pd.DataFrame({
            "timestamp": pd.Timestamp("2020-09-15"),
            "geo_id": "us",
            "cumulative_counts": 6589234.0,
            "new_counts": 6588748.0,
            "population": nation_pop,
            "incidence": 6588748 / nation_pop * INCIDENCE_BASE,
            "cumulative_prop": 6589234 / nation_pop * INCIDENCE_BASE
        }, index=[0])
        pd.testing.assert_frame_equal(test_df, expected_df)

        # Make sure the prop signals don't have inf values
        assert not nation_df["incidence"].eq(np.inf).any()
        assert not nation_df["cumulative_prop"].eq(np.inf).any()

    def test_msa_hrr(self, jhu_confirmed_test_data):
        for geo in ["msa", "hrr"]:
            test_df = jhu_confirmed_test_data
            new_df = geo_map(test_df, geo, "cumulative_prop")
            gmpr = GeoMapper()
            if geo == "msa":
                test_df = add_county_pop(test_df, gmpr)
                test_df = gmpr.replace_geocode(test_df, "fips", geo)
            if geo == "hrr":
                test_df = add_county_pop(test_df, gmpr)
                test_df = test_df[~test_df["fips"].str.endswith("000")]
                test_df = gmpr.replace_geocode(test_df, "fips", geo)

            new_df = new_df.set_index(["geo_id", "timestamp"]).sort_index()
            test_df = test_df.set_index([geo, "timestamp"]).sort_index()

            # Check that the non-proportional columns are identical
            assert new_df.eq(test_df)[["new_counts", "population", "cumulative_counts"]].all().all()
            # Check that the proportional signals are identical
            exp_incidence = test_df["new_counts"] / test_df["population"]  * INCIDENCE_BASE
            expected_cumulative_prop = test_df["cumulative_counts"] / test_df["population"] *\
                INCIDENCE_BASE
            assert new_df["incidence"].eq(exp_incidence).all()
            assert new_df["cumulative_prop"].eq(expected_cumulative_prop).all()
            # Make sure the prop signals don't have inf values
            assert not new_df["incidence"].eq(np.inf).any()
            assert not new_df["cumulative_prop"].eq(np.inf).any()

    def test_add_county_pop(self):
        gmpr = GeoMapper()
        test_df = pd.DataFrame({"fips": ["01001", "06000", "06097", "72000", "72153", "78000"]})
        state_pop = gmpr.get_crosswalk("state_code", "pop")
        pr_pop = int(state_pop.loc[state_pop.state_code == "78", "pop"])
        fips_pop = gmpr.get_crosswalk("fips", "pop")
        county01 = int(fips_pop.loc[fips_pop.fips == "01001", "pop"])
        county06 = int(fips_pop.loc[fips_pop.fips == "06097", "pop"])
        county72 = int(fips_pop.loc[fips_pop.fips == "72153", "pop"])
        df = add_county_pop(test_df, gmpr)
        expected_df = pd.DataFrame({"fips": ["01001", "06000", "06097", "72000", "72153", "78000"],
                          "population": [county01, np.nan, county06, np.nan, county72,  pr_pop]})
        pd.testing.assert_frame_equal(df, expected_df)
