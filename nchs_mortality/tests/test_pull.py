import pytest

from os.path import join

import pandas as pd
from delphi_utils import read_params

from delphi_nchs_mortality.pull import pull_nchs_mortality_data
from delphi_nchs_mortality.constants import METRICS

params = read_params()
export_start_date = params["export_start_date"]
export_dir = params["export_dir"]
static_file_dir = params["static_file_dir"]
token = params["token"]

map_df = pd.read_csv(
    join(static_file_dir, "state_pop.csv"), dtype={"fips": int}
)

class TestPullNCHS:
    def test_good_file(self):
        df = pull_nchs_mortality_data(token, map_df, "test_data.csv")
        
        # Test columns
        assert (df.columns.values == [
                'covid_deaths', 'total_deaths', 'percent_of_expected_deaths',
                'pneumonia_deaths', 'pneumonia_and_covid_deaths',
                'influenza_deaths', 'pneumonia_influenza_or_covid_19_deaths',
                "timestamp", "geo_id", "population"]).all()
    
        # Test aggregation for NYC and NY
        raw_df = pd.read_csv("./test_data/test_data.csv", parse_dates=["timestamp"])
        for metric in METRICS:
            ny_list = raw_df.loc[(raw_df["state"] == "New York")
                                & (raw_df[metric].isnull()), "timestamp"].values
            nyc_list = raw_df.loc[(raw_df["state"] == "New York City")
                                & (raw_df[metric].isnull()), "timestamp"].values
            final_list = df.loc[(df["geo_id"] == "ny")
                                & (df[metric].isnull()), "timestamp"].values
            assert set(final_list) == set(ny_list).intersection(set(nyc_list))

        # Test missing value
        for state, geo_id in zip(map_df["state"], map_df["geo_id"]):
            if state in set(["New York", "New York City"]):
                continue
            for metric in METRICS:
                test_list = raw_df.loc[(raw_df["state"] == state)
                                    & (raw_df[metric].isnull()), "timestamp"].values
                final_list = df.loc[(df["geo_id"] == geo_id)
                                    & (df[metric].isnull()), "timestamp"].values
                assert set(final_list) == set(test_list)

    def test_bad_file_with_inconsistent_time_col(self):
        with pytest.raises(ValueError):
            df = pull_nchs_mortality_data(token, map_df,
                                          "bad_data_with_inconsistent_time_col.csv")
      
    def test_bad_file_with_inconsistent_time_col(self):
        with pytest.raises(ValueError):
            df = pull_nchs_mortality_data(token, map_df,
                                          "bad_data_with_missing_cols.csv")

    
        
