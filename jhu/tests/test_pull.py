import pytest

from os.path import join

import pandas as pd
from jhu.pull import pull_jhu_data

pop_df = pd.read_csv(
    join("static", "fips_population.csv"), dtype={"fips": float, "population": float}
).rename({"fips": "FIPS"}, axis=1)


class TestPullJHU:
    def test_good_file(self):

        df = pull_jhu_data(join("test_dir", "small_{metric}.csv"), "deaths", pop_df)

        assert (
            df.columns.values
            == ["fips", "timestamp", "population", "new_counts", "cumulative_counts"]
        ).all()

    def test_missing_days(self):

        with pytest.raises(ValueError):
            df = pull_jhu_data(
                join("test_dir", "bad_{metric}_missing_days.csv"), "confirmed", pop_df
            )

    def test_missing_cols(self):

        with pytest.raises(ValueError):
            df = pull_jhu_data(
                join("test_dir", "bad_{metric}_missing_cols.csv"), "confirmed", pop_df
            )

    def test_extra_cols(self):

        with pytest.raises(ValueError):
            df = pull_jhu_data(
                join("test_dir", "bad_{metric}_extra_cols.csv"), "confirmed", pop_df
            )
