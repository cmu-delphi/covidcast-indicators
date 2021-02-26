# -*- coding: utf-8 -*-
from os import listdir, remove
from os.path import join

from boto3 import Session
from moto import mock_s3
import numpy as np
import pandas as pd
import pytest


@pytest.fixture(scope="session")
def clean_receiving_dir():
    # Clean receiving directory
    for fname in listdir("receiving"):
        if fname not in (".gitkeep", ".gitignore"):
            remove(join("receiving", fname))


@pytest.fixture
def CAN_parquet_data():
    columns = ["provider", "dt", "location_id", "location", "location_type", "variable_name",
            "measurement", "unit", "age", "race", "ethnicity", "sex", "last_updated", "value"]
    data = [
        ["cdc", "2021-01-01", "iso1:us#iso2:us-al#fips:01001", 1001, "county", "pcr_tests_positive",
            "rolling_average_7_day", "percentage", "all", "all", "all", "all", "2021-01-02 19:00:00", 50.0],
        ["cdc", "2021-01-01", "iso1:us#iso2:us-al#fips:01003", 1003, "county", "pcr_tests_positive",
            "rolling_average_7_day", "percentage", "all", "all", "all", "all", "2021-01-02 19:00:00", 25.0],
        ["cdc", "2021-01-01", "iso1:us#iso2:us-al#fips:01005", 1005, "county", "pcr_tests_positive",
            "rolling_average_7_day", "percentage", "all", "all", "all", "all", "2021-01-02 19:00:00", 50.0],

        ["cdc", "2021-01-01", "iso1:us#iso2:us-al#fips:01001", 1001, "county", "pcr_tests_total",
            "rolling_average_7_day", "specimens", "all", "all", "all", "all", "2021-01-02 19:00:00", 10.0],
        ["cdc", "2021-01-01", "iso1:us#iso2:us-al#fips:01003", 1003, "county", "pcr_tests_total",
            "rolling_average_7_day", "specimens", "all", "all", "all", "all", "2021-01-02 19:00:00", 20.0],
        ["cdc", "2021-01-01", "iso1:us#iso2:us-al#fips:01005", 1005, "county", "pcr_tests_total",
            "rolling_average_7_day", "specimens", "all", "all", "all", "all", "2021-01-02 19:00:00", 20.0],

        ["cdc", "2021-01-01", "iso1:us#iso2:us-pa#fips:42001", 42001, "county", "pcr_tests_positive",
            "rolling_average_7_day", "percentage", "all", "all", "all", "all", "2021-01-02 19:00:00", 50.0],
        ["cdc", "2021-01-01", "iso1:us#iso2:us-pa#fips:42003", 42003, "county", "pcr_tests_positive",
            "rolling_average_7_day", "percentage", "all", "all", "all", "all", "2021-01-02 19:00:00", 20.0],
        ["cdc", "2021-01-01", "iso1:us#iso2:us-pa#fips:42005", 42005, "county", "pcr_tests_positive",
            "rolling_average_7_day", "percentage", "all", "all", "all", "all", "2021-01-02 19:00:00", 10.0],

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
        ["01001", "2021-01-01", 5, 10, 0.5],
        ["01003", "2021-01-01", 5, 20, 0.25],
        ["01005", "2021-01-01", 10, 20, 0.5],
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
        ["al", "2021-01-01", 20, 50, 0.4],
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
        ["19300", "2021-01-01", 5, 20, 0.25],
        ["23900", "2021-01-01", 5, 10, 0.5],
        ["33860", "2021-01-01", 5, 10, 0.5],
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
        ["1", "2021-01-01", 0.195525, 0.391050, 0.5],
        ["134", "2021-01-01", 0.159989, 0.639958, 0.25],
        ["2", "2021-01-01", 9.743599, 19.487198, 0.5],
        ["351", "2021-01-01", 0.0145052, 0.145052, 0.1],
        ["352", "2021-01-01", 2.690298, 5.380595, 0.5],
        ["357", "2021-01-01", 4.985495, 29.854948, 0.166991],
        ["363", "2021-01-01", 2.309702, 4.619405, 0.5],
        ["6", "2021-01-01", 4.840011, 19.360042, 0.25],
        ["7", "2021-01-01", 5.060876, 10.121752, 0.5],
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
        ["4", "2021-01-01", 20, 50, 0.4],
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
        ["us", "2021-01-01", 30, 90, 30 / 90],
    ]

    df = pd.DataFrame(data, columns=columns)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    p, n = df.pcr_positivity_rate, df.pcr_tests_total
    df["se"] = np.sqrt(p * (1 - p) / n)

    return df
