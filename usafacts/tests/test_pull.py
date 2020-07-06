import pytest

from os.path import join

import pandas as pd
from delphi_usafacts.pull import pull_usafacts_data

pop_df = pd.read_csv(
    join("..", "static", "fips_population.csv"),
    dtype={"fips": float, "population": float}
).rename({"fips": "FIPS"}, axis=1)

base_url_good = "test_data/small_{metric}.csv"

base_url_bad = {
    "missing_days": "test_data/bad_{metric}_missing_days.csv",
    "missing_cols": "test_data/bad_{metric}_missing_cols.csv",
    "extra_cols": "test_data/bad_{metric}_extra_cols.csv"
}


class TestPullUSAFacts:
    def test_good_file(self):
        metric = "deaths"
        df = pull_usafacts_data(base_url_good, metric, pop_df)

        assert (
            df.columns.values
            == ["fips", "timestamp", "population", "new_counts", "cumulative_counts"]
        ).all()

    def test_missing_days(self):
        
        metric = "confirmed"
        with pytest.raises(ValueError):
            df = pull_usafacts_data(
                base_url_bad["missing_days"], metric, pop_df
            )

    def test_missing_cols(self):
        
        metric = "confirmed"
        with pytest.raises(ValueError):
            df = pull_usafacts_data(
                base_url_bad["missing_cols"], metric, pop_df
            )

    def test_extra_cols(self):

        metric = "confirmed"
        with pytest.raises(ValueError):
            df = pull_usafacts_data(
                base_url_bad["extra_cols"], metric, pop_df
            )