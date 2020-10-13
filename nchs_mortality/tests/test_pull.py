import pytest

from os.path import join

import pandas as pd
from delphi_utils import read_params

from delphi_nchs_mortality.pull import pull_nchs_mortality_data

params = read_params()
export_start_date = params["export_start_date"]
export_dir = params["export_dir"]
static_file_dir = params["static_file_dir"]
token = params["token"]

map_df = pd.read_csv(
    join(static_file_dir, "state_pop.csv"), dtype={"fips": int}
)

class TestPullUSAFacts:
    def test_good_file(self):
        df = pull_nchs_mortality_data(token, map_df, "test_data.csv")

        assert (df.columns.values == [
                'covid_deaths', 'total_deaths', 'percent_of_expected_deaths',
                'pneumonia_deaths', 'pneumonia_and_covid_deaths',
                'influenza_deaths', 'pneumonia_influenza_or_covid_19_deaths',
                "timestamp", "geo_id", "population"]).all()

    def test_bad_file_with_inconsistent_time_col(self):
        with pytest.raises(ValueError):
            df = pull_nchs_mortality_data(token, map_df,
                                          "bad_data_with_inconsistent_time_col.csv")
      
    def test_bad_file_with_inconsistent_time_col(self):
        with pytest.raises(ValueError):
            df = pull_nchs_mortality_data(token, map_df,
                                          "bad_data_with_missing_cols.csv")
