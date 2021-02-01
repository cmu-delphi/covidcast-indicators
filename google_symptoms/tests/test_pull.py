import pytest
import mock
from freezegun import freeze_time
from datetime import date, datetime
import pandas as pd

from delphi_google_symptoms.pull import (
    pull_gs_data, preprocess, format_dates_for_query, pull_gs_data_one_geolevel, get_date_range)
from delphi_google_symptoms.constants import METRICS, COMBINED_METRIC

good_input = {
    "state": "test_data/small_states_daily.csv",
    "county": "test_data/small_counties_daily.csv"
}

bad_input = {
    "missing_cols": "test_data/bad_state_missing_cols.csv",
    "invalid_fips": "test_data/bad_county_invalid_fips.csv"
}

symptom_names = ["symptom_" +
                 metric.replace(" ", "_") for metric in METRICS]
keep_cols = ["open_covid_region_code", "date"] + symptom_names
new_keep_cols = ["geo_id", "timestamp"] + METRICS + [COMBINED_METRIC]


# Set up fake list of dates to fetch.
dates = [
    "20200726",
    "20200811"
]
date_list = [datetime.strptime(date, "%Y%m%d").date() for date in dates]


class TestPullGoogleSymptoms:
    @freeze_time("2021-01-05")
    @mock.patch("pandas_gbq.read_gbq")
    @mock.patch("delphi_google_symptoms.pull.initialize_credentials")
    def test_good_file(self, mock_credentials, mock_read_gbq):
        # Set up fake data.
        state_data = pd.read_csv(
            good_input["state"], parse_dates=["date"])[keep_cols]
        county_data = pd.read_csv(
            good_input["county"], parse_dates=["date"])[keep_cols]

        # Mocks
        mock_read_gbq.side_effect = [state_data, county_data]
        mock_credentials.return_value = None

        dfs = pull_gs_data(
            "", datetime.strptime("20201230", "%Y%m%d"), 0)

        for level in ["county", "state"]:
            df = dfs[level]
            assert (
                df.columns.values
                == ["geo_id", "timestamp"] + METRICS + [COMBINED_METRIC]
            ).all()

            # combined_symptoms is nan when both Anosmia and Ageusia are nan
            assert sum(~df.loc[
                (df[METRICS[0]].isnull())
                & (df[METRICS[1]].isnull()), COMBINED_METRIC].isnull()) == 0
            # combined_symptoms is not nan when either Anosmia or Ageusia isn't nan
            assert sum(df.loc[
                (~df[METRICS[0]].isnull())
                & (df[METRICS[1]].isnull()), COMBINED_METRIC].isnull()) == 0
            assert sum(df.loc[
                (df[METRICS[0]].isnull())
                & (~df[METRICS[1]].isnull()), COMBINED_METRIC].isnull()) == 0

    def test_missing_cols(self):
        df = pd.read_csv(bad_input["missing_cols"])
        with pytest.raises(KeyError):
            preprocess(df, "state")

    def test_invalid_fips(self):
        df = pd.read_csv(bad_input["invalid_fips"])
        with pytest.raises(AssertionError):
            preprocess(df, "county")


class TestPullHelperFuncs:
    @freeze_time("2021-01-05")
    def test_get_date_range_recent_export_start_date(self):
        output = get_date_range(
            datetime.strptime("20201230", "%Y%m%d"),
            14
        )

        expected = [date(2020, 12, 24),
                    date(2021, 1, 5)]
        assert set(output) == set(expected)

    @freeze_time("2021-01-05")
    def test_get_date_range(self):
        output = get_date_range(
            datetime.strptime("20200201", "%Y%m%d"),
            14
        )

        expected = [date(2020, 12, 16), date(2021, 1, 5)]
        assert set(output) == set(expected)

    def test_format_dates_for_query(self):
        date_list = [date(2016, 12, 30), date(2021, 1, 5)]
        output = format_dates_for_query(date_list)
        expected = ["2016-12-30", "2021-01-05"]
        assert output == expected

    @mock.patch("pandas_gbq.read_gbq")
    def test_pull_one_gs_no_dates(self, mock_read_gbq):
        mock_read_gbq.return_value = pd.DataFrame()

        output = pull_gs_data_one_geolevel("state", ["", ""])
        expected = pd.DataFrame(columns=new_keep_cols)
        assert output.equals(expected)

    def test_preprocess_no_data(self):
        output = preprocess(pd.DataFrame(columns=keep_cols), "state")
        expected = pd.DataFrame(columns=new_keep_cols)
        assert output.equals(expected)
