"""Retrieve data and wrangle into appropriate format."""
# -*- coding: utf-8 -*-
import re

import numpy as np
import pandas as pd
from datetime import date, datetime
from os import listdir
from os.path import isfile, join
from collections import defaultdict

import pandas_gbq
from pandas_gbq.gbq import GenericGBQException
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound

from .constants import STATE_TO_ABBREV, DC_FIPS, METRICS, COMBINED_METRIC


# Create map of BigQuery to desired column names.
colname_map = {"symptom_" +
               metric.replace(" ", "_"): metric for metric in METRICS}


def preprocess(df, level):
    """
    Conforms the pulled data from Google COVID-19 Search Trends symptoms data into a dataset.

    The output dataset has:

    - Each row corresponding to (County/State, Date),
      denoted (FIPS/State abbrev, timestamp).
    - Each row additionally has columns corresponding to sensors such as
      "Anosmia" and "Ageusia".

    Parameters
    ----------
    df: pd.DataFrame
        Read from the raw url with column "geo_id" for state/fips
    level: str
        "county" or "state"

    Returns
    ---------
    pd.DataFrame
        Dataframe as described above.
    """
    # Constants
    KEEP_COLUMNS = ["geo_id", "date"] + METRICS + [COMBINED_METRIC]

    df[COMBINED_METRIC] = 0
    for metric in METRICS:
        df[COMBINED_METRIC] += df[metric].fillna(0)
    df.loc[
        (df["Anosmia"].isnull())
        & (df["Ageusia"].isnull()), COMBINED_METRIC] = np.nan

    # Delete rows with missing FIPS
    null_mask = (df["geo_id"].isnull())
    df = df.loc[~null_mask]

    # Confirm FIPS
    if level == "county":
        test_result = []
        for fips in df["geo_id"].values:
            test_result.append(len(re.findall(r"\b\d{5}\b", fips)) == 1)
        assert all(test_result)

    # keep necessary columns only
    try:
        df = df[KEEP_COLUMNS]
    except KeyError:
        raise ValueError(
            "Some necessary columns are missing. The dataset "
            "schema may have changed. Please investigate."
        )

    # Make sure each FIPS/state has same number of rows
    geo_list = df["geo_id"].unique()
    date_list = pd.date_range(
        start=df["date"].min(),
        end=df["date"].max(),
        freq='D')
    index_df = pd.MultiIndex.from_product(
        [geo_list, date_list], names=['geo_id', 'date']
    )
    df = df.set_index(
        ["geo_id", "date"]
    ).reindex(
        index_df
    ).reset_index(
    ).rename(
        {"date": "timestamp"}, axis=1)

    return df


def get_missing_dates(receiving_dir, export_start_date):
    """Decide which dates we want to retrieve data for based on existing
    CSVs.

    Parameters
    ----------
    receiving_dir: str
        path to output directory
    export_start_date: date
        first date to retrieve data for

    Returns
    -------
    list
    """
    OUTPUT_NAME_PATTERN = re.compile("^[0-9]{8}_.*[.]csv")
    existing_output_files = [f for f in listdir(receiving_dir) if isfile(
        join(receiving_dir, f)) and OUTPUT_NAME_PATTERN.match(f)]

    existing_output_dates = {datetime.strptime(f[0:8], "%Y%m%d").date()
                             for f in existing_output_files}
    expected_dates = {date.date() for date in pd.date_range(
        start=export_start_date,
        end=date.today(),
        freq='D')}

    missing_dates = list(expected_dates.difference(existing_output_dates))

    return missing_dates


def format_dates_for_query(date_list):
    """Turn list of dates into year-grouped dict formatted for use in
    BigQuery query.

    Parameters
    ----------
    dates_list: list
        collection of dates of days we want to pull data for

    Returns
    -------
    dict: {year: "timestamp(date), ..."}
    """
    earliest_available_symptom_search_year = 2017

    # Make a dictionary of years: list(dates).
    date_dict = defaultdict(list)
    for d in date_list:
        if d.year >= earliest_available_symptom_search_year:
            date_dict[d.year].append(datetime.strftime(d, "%Y-%m-%d"))

    # For each year, convert list of dates into list of BigQuery-
    # compatible timestamps.
    for key in date_dict.keys():
        date_dict[key] = 'timestamp("' + \
            '"), timestamp("'.join(date_dict[key]) + '")'

    return date_dict


def produce_query(level, year, dates_str):
    """Pull latest data for a single geo level and transform it into the
    appropriate format, as described in preprocess function.

    Parameters
    ----------
    level: str
        "county" or "state"
    year: int
        year of all dates in dates_str; used to specify table to pull data from
    dates_str: str
        "timestamp(date), ..." where timestamps are BigQuery-compatible

    Returns
    -------
    str
    """
    base_query = """
    select
        case
            when sub_region_2_code is null then sub_region_1_code
            when sub_region_2_code is not null then concat(sub_region_1_code, "-", sub_region_2_code)
        end as open_covid_region_code,
        date,
        {symptom_cols}
    from `bigquery-public-data.covid19_symptom_search.{symptom_table}`
    where timestamp(date) in ({date_list})
    """
    base_level_table = {"state": "states_daily_{year}",
                        "county": "counties_daily_{year}"}

    # Add custom values to base_query
    query = base_query.format(
        symptom_cols=", ".join(colname_map.keys()),
        symptom_table=base_level_table[level].format(year=year),
        date_list=dates_str)

    return(query)


def pull_gs_data_one_geolevel(level, dates_dict):
    """Pull latest data for a single geo level and transform it into the
    appropriate format, as described in preprocess function.

    Note that we retrieve state level data from "states_daily_<year>"
    where there are state level data for 51 states including 'District of Columbia'.

    We retrieve the county level data from "counties_daily_<year>"
    where there is county level data available except District of Columbia.
    We filter the data such that we only keep rows with valid FIPS.

    PS:  No information for PR

    Parameters
    ----------
    level: str
        "county" or "state"
    dates_dict: dict
        {year: "timestamp(date), ..."} where timestamps are BigQuery-compatible

    Returns
    -------
    pd.DataFrame
    """
    df = []
    for year in dates_dict.keys():
        query = produce_query(level, year, dates_dict[year])

        try:
            result = pandas_gbq.read_gbq(query, progress_bar_type=None)
        except GenericGBQException as e:
            if isinstance(e.__context__, NotFound):
                print(
                    "BigQuery table for year {year} not found".format(year=year))
                result = pd.DataFrame(
                    columns=["open_covid_region_code", "date"] + list(colname_map.keys()))
            else:
                raise(e)

        df.append(result)

    df = pd.concat(df)
    df.rename(colname_map, axis=1, inplace=True)
    df["geo_id"] = df["open_covid_region_code"].apply(
        lambda x: x.split("-")[-1].lower())
    df = preprocess(df, level)

    return(df)


def initialize_credentials(path_to_credentials):
    """ Provide pandas_gbq with BigQuery credentials

    Parameters
    ----------
    path_to_credentials: str
        Path to BigQuery API key and service account json file

    Returns
    -------
    None
    """
    credentials = service_account.Credentials.from_service_account_file(
        path_to_credentials)
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = credentials.project_id

    return


def pull_gs_data(path_to_credentials, receiving_dir, export_start_date):
    """Pull latest dataset for each geo level and combine.

    PS:  No information for PR

    Parameters
    ----------
    path_to_credentials: str
        Path to BigQuery API key and service account json file
    level: str
        "county" or "state"
    receiving_dir: str
        path to output directory
    export_start_date: date
        first date to retrieve data for

    Returns
    -------
    dict: {"county": pd.DataFrame, "state": pd.DataFrame}
    """

    # Fetch and format dates we want to attempt to retrieve
    missing_dates = get_missing_dates(receiving_dir, export_start_date)
    missing_dates_dict = format_dates_for_query(missing_dates)

    initialize_credentials(path_to_credentials)

    # Create dictionary for state and county level data
    dfs = {}

    # For state level data
    dfs["state"] = pull_gs_data_one_geolevel("state", missing_dates_dict)

    # For county level data
    dfs["county"] = pull_gs_data_one_geolevel("county", missing_dates_dict)

    # Add District of Columbia as county
    try:
        df_dc_county = dfs["state"][dfs["state"]["geo_id"] == "dc"].drop(
            "geo_id", axis=1)
        df_dc_county.loc[:, "geo_id"] = DC_FIPS
        dfs["county"] = dfs["county"].append(df_dc_county)
    except KeyError:
        pass

    return dfs
