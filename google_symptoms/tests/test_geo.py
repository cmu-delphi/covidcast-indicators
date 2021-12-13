import pytest

from os.path import join

import numpy as np
import pandas as pd

from delphi_google_symptoms.geo import geo_map
from delphi_google_symptoms.constants import METRICS, COMBINED_METRIC
from delphi_utils.geomap import GeoMapper

class TestGeo:
    def test_fips(self):
        df = pd.DataFrame(
            {
                "geo_id": ["53003", "48027", "50103"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15"],
                METRICS[0]: [10, 15, 2],
                METRICS[1]: [100, 20, 45],
                COMBINED_METRIC: [110, 35, 47],
            }
        )
        new_df = geo_map(df, "county")
        
        assert set(new_df.keys()) == set(df.keys())
        assert (new_df[METRICS[0]] == df[METRICS[0]]).all()
        assert (new_df[METRICS[1]] == df[METRICS[1]]).all()
        assert (new_df[COMBINED_METRIC] == df[COMBINED_METRIC]).all()
        
    def test_hrr(self):
        gmpr = GeoMapper()
        df = pd.DataFrame(
            {
                "geo_id": ["01001", "01009", "01007"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15"],
                METRICS[0]: [10, 15, 2],
                METRICS[1]: [100, 20, 45],
                COMBINED_METRIC: [110, 35, 47],
            }
        )

        fips2hrr = gmpr.add_population_column(gmpr.get_crosswalk("fips", "hrr"), "fips"
            ).assign(population = lambda x: x.weight * x.population
            ).drop("weight", axis="columns")
        hrr_pop = fips2hrr.groupby("hrr"
            ).sum(
            ).reset_index(
            ).rename(columns={"population": "hrr_pop"})
        df_plus = df.merge(fips2hrr, left_on="geo_id", right_on="fips", how="left"
            ).merge(hrr_pop, on="hrr", how="left"
            ).assign(
                fractional_pop = lambda x: x.population / x.hrr_pop,
                metric_0 = lambda x: x.fractional_pop * x[METRICS[0]],
                metric_1 = lambda x: x.fractional_pop * x[METRICS[1]],
                combined_metric = lambda x: x.metric_0 + x.metric_1
            ).groupby("hrr"
            ).sum(
            ).drop(
                labels=[METRICS[0], METRICS[1], COMBINED_METRIC],
                axis="columns"
            ).rename(
                columns={"metric_0": METRICS[0], "metric_1": METRICS[1], "combined_metric": COMBINED_METRIC}
            )

        new_df = geo_map(df, "hrr").dropna()

        assert set(new_df.keys()) == set(df.keys())
        assert set(new_df["geo_id"]) == set(["1", "5", "7", "9"])
        assert new_df[METRICS[0]].values == pytest.approx(df_plus[METRICS[0]].tolist())
        assert new_df[METRICS[1]].values == pytest.approx(df_plus[METRICS[1]].tolist())
        assert new_df[COMBINED_METRIC].values == pytest.approx(df_plus[COMBINED_METRIC].tolist())
        
    def test_msa(self):
        gmpr = GeoMapper()
        df = pd.DataFrame(
            {
                "geo_id": ["01001", "01009", "01007"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15"],
                METRICS[0]: [10, 15, 2],
                METRICS[1]: [100, 20, 45],
                COMBINED_METRIC: [110, 35, 47],
            }
        )

        fips2msa = gmpr.add_population_column(gmpr.get_crosswalk("fips", "msa"), "fips")
        msa_pop = fips2msa.groupby("msa"
            ).sum(
            ).reset_index(
            ).rename(columns={"population": "msa_pop"})
        df_plus = df.merge(fips2msa, left_on="geo_id", right_on="fips", how="left"
            ).merge(msa_pop, on="msa", how="left"
            ).assign(
                fractional_pop = lambda x: x.population / x.msa_pop,
                metric_0 = lambda x: x.fractional_pop * x[METRICS[0]],
                metric_1 = lambda x: x.fractional_pop * x[METRICS[1]],
                combined_metric = lambda x: x.metric_0 + x.metric_1
            ).groupby("msa"
            ).sum(
            ).drop(
                labels=[METRICS[0], METRICS[1], COMBINED_METRIC],
                axis="columns"
            ).rename(
                columns={"metric_0": METRICS[0], "metric_1": METRICS[1], "combined_metric": COMBINED_METRIC}
            )

        new_df = geo_map(df, "msa").dropna()

        assert set(new_df.keys()) == set(df.keys())
        assert set(new_df["geo_id"]) == set(["13820", "33860"])
        assert new_df[METRICS[0]].values == pytest.approx(df_plus[METRICS[0]].tolist())
        assert new_df[METRICS[1]].values == pytest.approx(df_plus[METRICS[1]].tolist())
        assert new_df[COMBINED_METRIC].values == pytest.approx(df_plus[COMBINED_METRIC].tolist())
        
    def test_hhs(self):
        gmpr = GeoMapper()
        df = pd.DataFrame(
            {
                "geo_id": ["01001", "01009", "01007"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15"],
                METRICS[0]: [10, 15, 2],
                METRICS[1]: [100, 20, 45],
                COMBINED_METRIC: [110, 35, 47],
            }
        )

        fips2hhs = gmpr.add_population_column(gmpr.get_crosswalk("fips", "hhs"), "fips")
        hhs_pop = fips2hhs.groupby("hhs"
            ).sum(
            ).reset_index(
            ).rename(columns={"population": "hhs_pop"})
        df_plus = df.merge(fips2hhs, left_on="geo_id", right_on="fips", how="left"
            ).merge(hhs_pop, on="hhs", how="left"
            ).assign(
                fractional_pop = lambda x: x.population / x.hhs_pop,
                metric_0 = lambda x: x.fractional_pop * x[METRICS[0]],
                metric_1 = lambda x: x.fractional_pop * x[METRICS[1]],
                combined_metric = lambda x: x.metric_0 + x.metric_1
            ).groupby("hhs"
            ).sum(
            ).drop(
                labels=[METRICS[0], METRICS[1], COMBINED_METRIC],
                axis="columns"
            ).rename(
                columns={"metric_0": METRICS[0], "metric_1": METRICS[1], "combined_metric": COMBINED_METRIC}
            )

        new_df = geo_map(df, "hhs").dropna()

        assert set(new_df.keys()) == set(df.keys())
        assert set(new_df["geo_id"]) == set(["4"])
        assert new_df[METRICS[0]].values == pytest.approx(df_plus[METRICS[0]].tolist())
        assert new_df[METRICS[1]].values == pytest.approx(df_plus[METRICS[1]].tolist())
        assert new_df[COMBINED_METRIC].values == pytest.approx(df_plus[COMBINED_METRIC].tolist())
    
    def test_nation(self):
        gmpr = GeoMapper()
        df = pd.DataFrame(
            {
                "geo_id": ["01001", "01009", "01007"],
                "timestamp": ["2020-02-15", "2020-02-15", "2020-02-15"],
                METRICS[0]: [10, 15, 2],
                METRICS[1]: [100, 20, 45],
                COMBINED_METRIC: [110, 35, 47],
            }
        )

        fips2nation = gmpr.add_population_column(gmpr.get_crosswalk("fips", "hhs"), "fips")
        fips2nation.rename({"hhs": "nation"}, axis=1, inplace=True)
        fips2nation["nation"] = "nation"
        nation_pop = fips2nation.groupby("nation"
            ).sum(
            ).reset_index(
            ).rename(columns={"population": "nation_pop"})
        df_plus = df.merge(fips2nation, left_on="geo_id", right_on="fips", how="left"
            ).merge(nation_pop, on="nation", how="left"
            ).assign(
                fractional_pop = lambda x: x.population / x.nation_pop,
                metric_0 = lambda x: x.fractional_pop * x[METRICS[0]],
                metric_1 = lambda x: x.fractional_pop * x[METRICS[1]],
                combined_metric = lambda x: x.metric_0 + x.metric_1
            ).groupby("nation"
            ).sum(
            ).drop(
                labels=[METRICS[0], METRICS[1], COMBINED_METRIC],
                axis="columns"
            ).rename(
                columns={"metric_0": METRICS[0], "metric_1": METRICS[1], "combined_metric": COMBINED_METRIC}
            )

        new_df = geo_map(df, "nation").dropna()

        assert set(new_df.keys()) == set(df.keys())
        assert set(new_df["geo_id"]) == set(["nation"])
        assert new_df[METRICS[0]].values == pytest.approx(df_plus[METRICS[0]].tolist())
        assert new_df[METRICS[1]].values == pytest.approx(df_plus[METRICS[1]].tolist())
        assert new_df[COMBINED_METRIC].values == pytest.approx(df_plus[COMBINED_METRIC].tolist())

