import pytest

import pandas as pd
from delphi_utils.geomap import GeoMapper

from delphi_nchs_mortality.pull import pull_nchs_mortality_data, standardize_columns
from delphi_nchs_mortality.constants import METRICS

# export_start_date = PARAMS["indicator"]["export_start_date"]
EXPORT_DIR = "./receiving"
TOKEN = ""


class TestPullNCHS:
    def test_standardize_columns(self):
        df = standardize_columns(
            pd.DataFrame({
                "start_week": [1],
                "covid_deaths": [2],
                "pneumonia_and_covid_deaths": [4],
                "pneumonia_influenza_or_covid_19_deaths": [8]
            }))
        expected = pd.DataFrame({
            "timestamp": [1],
            "covid_19_deaths": [2],
            "pneumonia_and_covid_19_deaths": [4],
            "pneumonia_influenza_or_covid_19_deaths": [8]
        })
        pd.testing.assert_frame_equal(expected, df)

    def test_good_file(self):
        df = pull_nchs_mortality_data(TOKEN, "test_data.csv")

        # Test columns
        assert (df.columns.values == [
                'covid_19_deaths', 'total_deaths', 'percent_of_expected_deaths',
                'pneumonia_deaths', 'pneumonia_and_covid_19_deaths',
                'influenza_deaths', 'pneumonia_influenza_or_covid_19_deaths',
                "timestamp", "geo_id", "population"]).all()

        # Test aggregation for NYC and NY
        raw_df = pd.read_csv("./test_data/test_data.csv", parse_dates=["start_week"])
        raw_df = standardize_columns(raw_df)
        for metric in METRICS:
            ny_list = raw_df.loc[(raw_df["state"] == "New York")
                                & (raw_df[metric].isnull()), "timestamp"].values
            nyc_list = raw_df.loc[(raw_df["state"] == "New York City")
                                & (raw_df[metric].isnull()), "timestamp"].values
            final_list = df.loc[(df["geo_id"] == "ny")
                                & (df[metric].isnull()), "timestamp"].values
            assert set(final_list) == set(ny_list).intersection(set(nyc_list))

        # Test missing value
        gmpr = GeoMapper()
        state_ids = pd.DataFrame(list(gmpr.get_geo_values("state_id")))
        state_names = gmpr.replace_geocode(state_ids,
                                           "state_id",
                                           "state_name",
                                           from_col=0,
                                           date_col=None)
        for state, geo_id in zip(state_names, state_ids):
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
            pull_nchs_mortality_data(TOKEN, "bad_data_with_inconsistent_time_col.csv")

    def test_bad_file_with_missing_cols(self):
        with pytest.raises(ValueError):
            pull_nchs_mortality_data(TOKEN, "bad_data_with_missing_cols.csv")
