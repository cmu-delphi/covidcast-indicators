import pytest

from os.path import join

import pandas as pd
import numpy as np
from delphi_utils import GeoMapper
from delphi_usafacts.pull import pull_usafacts_data

base_url_good = "test_data/small_{metric}_pull.csv"

base_url_bad = {
    "missing_days": "test_data/bad_{metric}_missing_days.csv",
    "missing_cols": "test_data/bad_{metric}_missing_cols.csv",
    "extra_cols": "test_data/bad_{metric}_extra_cols.csv"
}

geo_mapper = GeoMapper()


class TestPullUSAFacts:
    def test_good_file(self):
        metric = "deaths"
        df = pull_usafacts_data(base_url_good, metric, geo_mapper)
        expected_df = pd.DataFrame({
            "fips": ["00001", "00001", "00001", "36009", "36009", "36009"],
            "timestamp": [pd.Timestamp("2020-02-29"), pd.Timestamp("2020-03-01"),
                          pd.Timestamp("2020-03-02"), pd.Timestamp("2020-02-29"),
                          pd.Timestamp("2020-03-01"), pd.Timestamp("2020-03-02")],
            "new_counts": [0., 0., 1., 2., 2., 2.],
            "cumulative_counts": [0, 0, 1, 2, 4, 6]},
            index=[1, 2, 3, 5, 6, 7])
        # sort since rows order doesn't matter
        pd.testing.assert_frame_equal(df.sort_index(), expected_df.sort_index())

    def test_missing_days(self):
        
        metric = "confirmed"
        with pytest.raises(ValueError):
            df = pull_usafacts_data(
                base_url_bad["missing_days"], metric, geo_mapper
            )

    def test_missing_cols(self):
        
        metric = "confirmed"
        with pytest.raises(ValueError):
            df = pull_usafacts_data(
                base_url_bad["missing_cols"], metric, geo_mapper
            )

    def test_extra_cols(self):

        metric = "confirmed"
        with pytest.raises(ValueError):
            df = pull_usafacts_data(
                base_url_bad["extra_cols"], metric, geo_mapper
            )