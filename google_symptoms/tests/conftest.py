# -*- coding: utf-8 -*-

import pytest
import mock
import pandas as pd

from os import listdir, remove, makedirs
from os.path import join, exists
from datetime import datetime

import delphi_google_symptoms
from delphi_google_symptoms.constants import METRICS


# Set up fake data so that tests don't require BigQuery
# credentials to run.

# State data is created by running the following query in the BigQuery
# browser console:
# select
#     case
#         when sub_region_2_code is null then sub_region_1_code
#         when sub_region_2_code is not null then concat(sub_region_1_code, "-", sub_region_2_code)
#     end as open_covid_region_code,
#     *
# from `bigquery-public-data.covid19_symptom_search.states_daily_2020` # States by day
# where timestamp(date) between timestamp("2020-07-26") and timestamp("2020-08-11")

# County data is created by running the following query in the BigQuery
# browser console:
# select
#     case
#         when sub_region_2_code is null then sub_region_1_code
#         when sub_region_2_code is not null then concat(sub_region_1_code, "-", sub_region_2_code)
#     end as open_covid_region_code,
#     *
# from `bigquery-public-data.covid19_symptom_search.counties_daily_2020` # Counties by day; includes state and county name, + FIPS code
# where timestamp(date) between timestamp("2020-07-26") and timestamp("2020-08-11")

good_input = {
    "state": "test_data/small_states_daily.csv",
    "county": "test_data/small_counties_daily.csv"
}

symptom_names = ["symptom_" +
                 metric.replace(" ", "_") for metric in METRICS]
keep_cols = ["open_covid_region_code", "date"] + symptom_names

state_data = pd.read_csv(good_input["state"], parse_dates=["date"])[keep_cols]
county_data = pd.read_csv(
    good_input["county"], parse_dates=["date"])[keep_cols]


# Set up fake list of dates to fetch.
dates = [
    "20200726",
    "20200811"
]

date_list = [datetime.strptime(date, "%Y%m%d").date() for date in dates]


@pytest.fixture(scope="session")
def run_as_module():
    params = {
        "common": {
            "export_dir": "./receiving"
        },
        "indicator": {
            "export_start_date": "2020-02-20",
            "num_export_days": 14,
            "bigquery_credentials": {},
            "static_file_dir": "../static"
        }
    }

    if exists("receiving"):
        # Clean receiving directory
        for fname in listdir("receiving"):
            remove(join("receiving", fname))
    else:
        makedirs("receiving")

    with mock.patch("delphi_google_symptoms.pull.get_date_range",
                    return_value=date_list) as mock_all_dates:
        with mock.patch("delphi_google_symptoms.pull.initialize_credentials",
                        return_value=None) as mock_credentials:
            with mock.patch("pandas_gbq.read_gbq", side_effect=[
                    state_data, county_data]) as mock_read_gbq:
                delphi_google_symptoms.run.run_module(params)
