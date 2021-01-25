# -*- coding: utf-8 -*-
from os import listdir, remove
from os.path import join

from boto3 import Session
from moto import mock_s3
import numpy as np
import pandas as pd
import pytest

from delphi_utils import read_params
from delphi_covid_act_now.run import run_module

@pytest.fixture(scope="session")
def run_as_module():
    # Clean receiving directory
    for fname in listdir("receiving"):
        if fname != ".gitkeep":
            remove(join("receiving", fname))

    with mock_s3():
        # Create the fake bucket we will be using
        params = read_params()
        aws_credentials = params["aws_credentials"]
        s3_client = Session(**aws_credentials).client("s3")
        s3_client.create_bucket(Bucket=params["bucket_name"])

        run_module()

@pytest.fixture
def CAN_parquet_data():
    columns = ["provider", "dt", "location_id", "location", "location_type", "variable_name",
            "measurement", "unit", "age", "race", "ethnicity", "sex", "last_updated", "value"]
    data = [
        ["cdc", "2021-01-01", "iso1:us#iso2:us-al#fips:01001", 1001, "county", "pcr_tests_positive",
            "rolling_average_7_day", "percentage", "all", "all", "all", "all", "2021-01-02 19:00:00", 0.0],
        ["cdc", "2021-01-01", "iso1:us#iso2:us-al#fips:01003", 1003, "county", "pcr_tests_positive",
            "rolling_average_7_day", "percentage", "all", "all", "all", "all", "2021-01-02 19:00:00", 0.0],
        ["cdc", "2021-01-01", "iso1:us#iso2:us-al#fips:01005", 1005, "county", "pcr_tests_positive",
            "rolling_average_7_day", "percentage", "all", "all", "all", "all", "2021-01-02 19:00:00", 0.0],

        ["cdc", "2021-01-01", "iso1:us#iso2:us-al#fips:01001", 1001, "county", "pcr_tests_total",
            "rolling_average_7_day", "specimens", "all", "all", "all", "all", "2021-01-02 19:00:00", 0.0],
        ["cdc", "2021-01-01", "iso1:us#iso2:us-al#fips:01003", 1003, "county", "pcr_tests_total",
            "rolling_average_7_day", "specimens", "all", "all", "all", "all", "2021-01-02 19:00:00", 0.0],
        ["cdc", "2021-01-01", "iso1:us#iso2:us-al#fips:01005", 1005, "county", "pcr_tests_total",
            "rolling_average_7_day", "specimens", "all", "all", "all", "all", "2021-01-02 19:00:00", 0.0],

        ["cdc", "2021-01-01", "iso1:us#iso2:us-pa#fips:42001", 42001, "county", "pcr_tests_positive",
            "rolling_average_7_day", "percentage", "all", "all", "all", "all", "2021-01-02 19:00:00", 0.5],
        ["cdc", "2021-01-01", "iso1:us#iso2:us-pa#fips:42003", 42003, "county", "pcr_tests_positive",
            "rolling_average_7_day", "percentage", "all", "all", "all", "all", "2021-01-02 19:00:00", 0.2],
        ["cdc", "2021-01-01", "iso1:us#iso2:us-pa#fips:42005", 42005, "county", "pcr_tests_positive",
            "rolling_average_7_day", "percentage", "all", "all", "all", "all", "2021-01-02 19:00:00", 0.1],

        ["cdc", "2021-01-01", "iso1:us#iso2:us-pa#fips:42001", 42001, "county", "pcr_tests_total",
            "rolling_average_7_day", "specimens", "all", "all", "all", "all", "2021-01-02 19:00:00", 10.0],
        ["cdc", "2021-01-01", "iso1:us#iso2:us-pa#fips:42003", 42003, "county", "pcr_tests_total",
            "rolling_average_7_day", "specimens", "all", "all", "all", "all", "2021-01-02 19:00:00", 20.0],
        ["cdc", "2021-01-01", "iso1:us#iso2:us-pa#fips:42005", 42005, "county", "pcr_tests_total",
            "rolling_average_7_day", "specimens", "all", "all", "all", "all", "2021-01-02 19:00:00", 10.0],

        ["SOME_SOURCE", "2021-01-15", "iso1:us#iso2:us-fl#fips:12093", 12093, "county", "SOME_OTHER_METRIC",
            "SOME_MEASUREMENT", "SOME_UNITS", "all", "all", "all", "all", "2021-01-21 19:00:00", 123.0],
    ]

    df_pq = pd.DataFrame(data, columns=columns)

    return df_pq

@pytest.fixture
def CAN_county_testing_data():
    columns = ["fips", "timestamp", "pcr_tests_positive", "pcr_tests_total", "pcr_positivity_rate"]
    data = [
        ["01001", "2021-01-01", 0, 0, 0],
        ["01003", "2021-01-01", 0, 0, 0],
        ["01005", "2021-01-01", 0, 0, 0],
        ["42001", "2021-01-01", 5, 10, 0.5],
        ["42003", "2021-01-01", 4, 20, 0.2],
        ["42005", "2021-01-01", 1, 10, 0.1],
    ]

    df = pd.DataFrame(data, columns=columns)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    p, n = df.pcr_positivity_rate, df.pcr_tests_total
    df["se"] = np.sqrt(p * (1 - p) / n)

    return df

@pytest.fixture
def CAN_state_testing_data():
    columns = ["fips", "timestamp", "pcr_tests_positive", "pcr_tests_total", "pcr_positivity_rate"]
    data = [
        ["al", "2021-01-01", 0, 0, 0],
        ["pa", "2021-01-01", 10, 40, 0.25]
    ]

    df = pd.DataFrame(data, columns=columns)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    p, n = df.pcr_positivity_rate, df.pcr_tests_total
    df["se"] = np.sqrt(p * (1 - p) / n)

    return df

@pytest.fixture
def CAN_msa_testing_data():
    columns = ["fips", "timestamp", "pcr_tests_positive", "pcr_tests_total", "pcr_positivity_rate"]
    data = [
        ["19300", "2021-01-01", 0, 0, 0],
        ["23900", "2021-01-01", 5, 10, 0.5],
        ["33860", "2021-01-01", 0, 0, 0],
        ["38300", "2021-01-01", 5, 30, 5 / 30],
    ]

    df = pd.DataFrame(data, columns=columns)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    p, n = df.pcr_positivity_rate, df.pcr_tests_total
    df["se"] = np.sqrt(p * (1 - p) / n)

    return df

@pytest.fixture
def CAN_hrr_testing_data():
    columns = ["fips", "timestamp", "pcr_tests_positive", "pcr_tests_total", "pcr_positivity_rate"]
    data = [
        ["1", "2021-01-01", 0, 0, 0],
        ["134", "2021-01-01", 0, 0, 0],
        ["2", "2021-01-01", 0, 0, 0],
        ["351", "2021-01-01", 0.0145052, 0.145052, 0.1],
        ["352", "2021-01-01", 2.690298, 5.380595, 0.5],
        ["357", "2021-01-01", 4.985495, 29.854948, 0.166991],
        ["363", "2021-01-01", 2.309702, 4.619405, 0.5],
        ["6", "2021-01-01", 0, 0, 0],
        ["7", "2021-01-01", 0, 0, 0],
    ]

    df = pd.DataFrame(data, columns=columns)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    p, n = df.pcr_positivity_rate, df.pcr_tests_total
    df["se"] = np.sqrt(p * (1 - p) / n)

    return df

@pytest.fixture
def CAN_hhs_testing_data():
    columns = ["fips", "timestamp", "pcr_tests_positive", "pcr_tests_total", "pcr_positivity_rate"]
    data = [
        ["3", "2021-01-01", 10, 40, 0.25],
        ["4", "2021-01-01", 0, 0, 0],
    ]

    df = pd.DataFrame(data, columns=columns)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    p, n = df.pcr_positivity_rate, df.pcr_tests_total
    df["se"] = np.sqrt(p * (1 - p) / n)

    return df

@pytest.fixture
def CAN_nation_testing_data():
    columns = ["fips", "timestamp", "pcr_tests_positive", "pcr_tests_total", "pcr_positivity_rate"]
    data = [
        ["us", "2021-01-01", 10, 40, 0.25],
    ]

    df = pd.DataFrame(data, columns=columns)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    p, n = df.pcr_positivity_rate, df.pcr_tests_total
    df["se"] = np.sqrt(p * (1 - p) / n)

    return df
