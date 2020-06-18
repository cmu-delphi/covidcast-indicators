import pytest

from os.path import join

import pandas as pd
from delphi_usafacts.pull import pull_usafacts_data

pop_df = pd.read_csv(
    join("..", "static", "fips_population.csv"),
    dtype={"fips": float, "population": float}
).rename({"fips": "FIPS"}, axis=1)

base_url_good = {
    "confirmed": "test_data/small_confirmed.csv",
    "deaths": "test_data/small_deaths.csv"
    }

base_url_bad = {
    "confirmed": "test_data/bad_confirmed.csv",
    "deaths": "test_data/bad_deaths.csv"
    }


class TestPullUSAFacts:
    def test_good_file(self):
        metric = "deaths"
        df = pull_usafacts_data(base_url_good[metric], metric, pop_df)

        assert (
            df.columns.values
            == ["fips", "timestamp", "population", "new_counts", "cumulative_counts"]
        ).all()

    def test_missing_days(self):
        
        metric = "confirmed"
        with pytest.raises(ValueError):
            df = pull_usafacts_data(
                base_url_bad[metric], metric, pop_df
            )

    def test_missing_cols(self):
        
        metric = "confirmed"
        with pytest.raises(ValueError):
            df = pull_usafacts_data(
                base_url_bad[metric], metric, pop_df
            )

    def test_extra_cols(self):

        metric = "confirmed"
        with pytest.raises(ValueError):
            df = pull_usafacts_data(
                base_url_bad[metric], metric, pop_df
            )