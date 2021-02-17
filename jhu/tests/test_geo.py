import pytest

import numpy as np
import pandas as pd
from delphi_utils import GeoMapper

from delphi_jhu.geo import geo_map, INCIDENCE_BASE

class TestGeoMap:
    def test_incorrect_geo(self, jhu_confirmed_test_data):
        df = jhu_confirmed_test_data

        with pytest.raises(ValueError):
            geo_map(df, "département", "cumulative_prop")

    def test_fips(self, jhu_confirmed_test_data):
        test_df = jhu_confirmed_test_data
        new_df = geo_map(test_df, "county", "cumulative_prop")

        pd.testing.assert_frame_equal(new_df.loc[(new_df.geo_id == "01001") &
                                                 (new_df.timestamp == "2020-09-15")],
                                      pd.DataFrame({"geo_id": "01001",
                                                    "timestamp": pd.Timestamp("2020-09-15"),
                                                    "cumulative_counts": 1463.0,
                                                    "new_counts": 1463.0,
                                                    "population": 55869,
                                                    "incidence": 1463 / 55869 * 100000,
                                                    "cumulative_prop": 1463 / 55869 * 100000},
                                                   index=[0]),
                                      )
        # Make sure the prop signals don't have inf values
        assert not new_df["incidence"].eq(np.inf).any()
        assert not new_df["cumulative_prop"].eq(np.inf).any()

        # make sure no megafips reported
        assert not any(i[0].endswith("000") for i in new_df.geo_id)

    def test_state_hhs_nation(self, jhu_confirmed_test_data):
        df = jhu_confirmed_test_data
        state_df = geo_map(df, "state", "cumulative_prop")
        pd.testing.assert_frame_equal(state_df.loc[(state_df.geo_id == "al") &
                                                   (state_df.timestamp == "2020-09-15")],
                                      pd.DataFrame({"timestamp": pd.Timestamp("2020-09-15"),
                                                    "geo_id": "al",
                                                    "cumulative_counts": 140160.0,
                                                    "new_counts": 140160.0,
                                                    "population": 4903185.0,
                                                    "incidence": 140160 / 4903185 * 100000,
                                                    "cumulative_prop": 140160 / 4903185 * 100000},
                                                   index=[1])
                                      )
        # Make sure the prop signals don't have inf values
        assert not state_df["incidence"].eq(np.inf).any()
        assert not state_df["cumulative_prop"].eq(np.inf).any()

        hhs_df = geo_map(df, "hhs", "cumulative_prop")
        pd.testing.assert_frame_equal(hhs_df.loc[(hhs_df.geo_id == "1") &
                                                 (hhs_df.timestamp == "2020-09-15")],
                                      pd.DataFrame({"timestamp": pd.Timestamp("2020-09-15"),
                                                    "geo_id": "1",
                                                    "cumulative_counts": 218044.0,
                                                    "new_counts": 218044.0,
                                                    "population": 14845063.0,
                                                    "incidence": 218044 / 14845063 * 100000,
                                                    "cumulative_prop": 218044 / 14845063 * 100000},
                                                   index=[0])
                                      )
        # Make sure the prop signals don't have inf values
        assert not hhs_df["incidence"].eq(np.inf).any()
        assert not hhs_df["cumulative_prop"].eq(np.inf).any()

        nation_df = geo_map(df, "nation", "cumulative_prop")
        pd.testing.assert_frame_equal(nation_df.loc[(nation_df.geo_id == "us") &
                                                    (nation_df.timestamp == "2020-09-15")],
                                      pd.DataFrame({"timestamp": pd.Timestamp("2020-09-15"),
                                                    "geo_id": "us",
                                                    "cumulative_counts": 6588732.0,
                                                    "new_counts": 6588732.0,
                                                    "population": 331940098.0,
                                                    "incidence": 6588732 / 331940098 * 100000,
                                                    "cumulative_prop": 6588732 / 331940098. *\
                                                                       100000},
                                                   index=[0])
                                      )
        # Make sure the prop signals don't have inf values
        assert not nation_df["incidence"].eq(np.inf).any()
        assert not nation_df["cumulative_prop"].eq(np.inf).any()

    def test_msa_hrr(self, jhu_confirmed_test_data):
        for geo in ["msa", "hrr"]:
            test_df = jhu_confirmed_test_data
            new_df = geo_map(test_df, geo, "cumulative_prop")
            gmpr = GeoMapper()
            test_df = gmpr.add_population_column(test_df, "fips")
            test_df = gmpr.replace_geocode(test_df, "fips", geo, date_col="timestamp")

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
