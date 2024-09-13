# -*- coding: utf-8 -*-
import logging
from pathlib import Path
import re

import copy
import pytest
import mock
import pandas as pd

from os import listdir, remove, makedirs
from os.path import join, exists

import delphi_google_symptoms
from delphi_google_symptoms.constants import METRICS

TEST_DIR = Path(__file__).parent

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
# where timestamp(date) between timestamp("2020-07-15") and timestamp("2020-08-22")

# County data is created by running the following query in the BigQuery
# browser console:
# select
#     case
#         when sub_region_2_code is null then sub_region_1_code
#         when sub_region_2_code is not null then concat(sub_region_1_code, "-", sub_region_2_code)
#     end as open_covid_region_code,
#     *
# from `bigquery-public-data.covid19_symptom_search.counties_daily_2020` # Counties by day; includes state and county name, + FIPS code
# where timestamp(date) between timestamp("2020-07-15") and timestamp("2020-08-22")

good_input = {
    "state": f"{TEST_DIR}/test_data/small_states_2020_07_15_2020_08_22.csv",
    "county": f"{TEST_DIR}/test_data/small_counties_2020_07_15_2020_08_22.csv"
}

patch_input = {
    "state": f"{TEST_DIR}/test_data/state_2024-05-16_2024-07-18.csv",
}

symptom_names = ["symptom_" +
                 metric.replace(" ", "_") for metric in METRICS]
keep_cols = ["open_covid_region_code", "date"] + symptom_names

state_data = pd.read_csv(good_input["state"], parse_dates=["date"])[keep_cols]
county_data = pd.read_csv(
    good_input["county"], parse_dates=["date"])[keep_cols]

state_data_gap = pd.read_csv(patch_input["state"], parse_dates=["date"])[keep_cols]

covidcast_backfill_metadata = pd.read_csv(f"{TEST_DIR}/test_data/covid_metadata_backfill.csv",
                                          parse_dates=["max_time", "min_time", "max_issue", "last_update"])
covidcast_metadata = pd.read_csv(f"{TEST_DIR}/test_data/covid_metadata.csv",
                                 parse_dates=["max_time", "min_time", "max_issue", "last_update"])

NEW_DATE = "2024-02-20"
@pytest.fixture(scope="session")
def logger():
    return logging.getLogger()

@pytest.fixture(scope="session")
def params():
    params = {
        "common": {
            "export_dir": f"{TEST_DIR}/receiving",
            "log_filename": f"{TEST_DIR}/test.log",
        },
        "indicator": {
            "bigquery_credentials": {},
            "num_export_days": 14,
            "custom_run": False,
            "static_file_dir": "../static",
            "api_credentials": "fakesecret"
        },
        "validation": {
            "common": {
                "span_length": 14,
                "min_expected_lag": {"all": "3"},
                "max_expected_lag": {"all": "4"},
            }
        }
    }
    return copy.deepcopy(params)

@pytest.fixture
def params_w_patch(params):
    params_copy = copy.deepcopy(params)
    params_copy["patch"] = {
            "start_issue": "2024-06-27",
            "end_issue": "2024-06-29",
            "patch_dir": "./patch_dir"
        }
    return params_copy


@pytest.fixture
def params_w_no_date(params):
    params_copy = copy.deepcopy(params)
    params_copy["indicator"]["num_export_days"] = None
    return params_copy


@pytest.fixture(scope="session")
def run_as_module(params):
    if exists("receiving"):
        # Clean receiving directory
        for fname in listdir("receiving"):
            remove(join("receiving", fname))
    else:
        makedirs("receiving")

    with mock.patch("delphi_google_symptoms.pull.initialize_credentials",
                    return_value=None), \
         mock.patch("pandas_gbq.read_gbq") as mock_read_gbq, \
         mock.patch("delphi_google_symptoms.pull.initialize_credentials", return_value=None), \
         mock.patch("delphi_google_symptoms.date_utils.Epidata.covidcast_meta", return_value=None) as mock_covidcast_meta:
        def side_effect(*args, **kwargs):
            if "symptom_search_sub_region_1_daily" in args[0]:
                df = state_data
                pattern = re.compile(r'\d{4}-\d{2}-\d{2}')
                start_date, end_date = re.findall(pattern, args[0])
                return df[(df["date"] >= start_date) & (df["date"] <= end_date)]
            elif "symptom_search_sub_region_2_daily" in args[0]:
                df = county_data
                pattern = re.compile(r'\d{4}-\d{2}-\d{2}')
                start_date, end_date = re.findall(pattern, args[0])
                return df[(df["date"] >= start_date) & (df["date"] <= end_date)]
            else:
                return pd.DataFrame()

        mock_read_gbq.side_effect = side_effect
        delphi_google_symptoms.run.run_module(params)
