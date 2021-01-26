# -*- coding: utf-8 -*-

import pytest
import mock
import pandas as pd

from os import listdir, remove, makedirs
from os.path import join, exists
from datetime import datetime

import delphi_google_symptoms
from delphi_google_symptoms.constants import METRICS


# Set up fake data.
good_input = {
    "state": "test_data/202008_states_daily.csv",
    "county": "test_data/202008_counties_daily.csv"
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
    "20200727",
    "20200728",
    "20200729",
    "20200730",
    "20200731",
    "20200801",
    "20200802",
    "20200803",
    "20200804",
    "20200805",
    "20200806",
    "20200807",
    "20200808",
    "20200809",
    "20200810",
    "20200811"
]

date_list = [datetime.strptime(date, "%Y%m%d").date() for date in dates]


@pytest.fixture(scope="session")
def run_as_module():
    if exists("receiving"):
        # Clean receiving directory
        for fname in listdir("receiving"):
            remove(join("receiving", fname))
    else:
        makedirs("receiving")

    with mock.patch("delphi_google_symptoms.pull.get_missing_dates",
                    return_value=date_list) as mock_missing_dates:
        with mock.patch("delphi_google_symptoms.pull.initialize_credentials",
                        return_value=None) as mock_credentials:
            with mock.patch("pandas_gbq.read_gbq", side_effect=[
                    state_data, county_data]) as mock_read_gbq:
                delphi_google_symptoms.run.run_module()
