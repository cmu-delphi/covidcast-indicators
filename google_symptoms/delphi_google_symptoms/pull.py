"""Retrieve data and wrangle into appropriate format."""
# -*- coding: utf-8 -*-
import re

from datetime import date, datetime, timedelta
from os import listdir, makedirs
from os.path import isfile, join, exists
from collections import defaultdict

import pandas_gbq
from pandas_gbq.gbq import GenericGBQException
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
import numpy as np
import pandas as pd

from .constants import DC_FIPS, METRICS, COMBINED_METRIC


# Create map of BigQuery symptom column names to desired column names.
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

    df.rename(colname_map, axis=1, inplace=True)
    df["geo_id"] = df["open_covid_region_code"].apply(
        lambda x: x.split("-")[-1].lower())

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

    if len(df) != 0:
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
        )

    df = df.rename({"date": "timestamp"}, axis=1)

    return df


def get_missing_dates(receiving_dir, export_start_date):
    """Produce list of dates to retrieve data for.

    Date list is created based on dates seen in already exported CSVs.

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
    if not exists(receiving_dir):
        makedirs(receiving_dir)

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


def get_all_dates(receiving_dir, export_start_date):
    """Pad missing dates with enough extra days to do smoothing.

    Using the missing_dates list as reference, creates a new list of dates
    spanning 6 days before the earliest date in missing_dates to today. This
    pads missing_dates with enough prior days to produce smoothed estimates
    starting on min(missing_dates) and fills in any gaps in missing_dates.

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
    PAD_DAYS = 7

    missing_dates = get_missing_dates(receiving_dir, export_start_date)
    if len(missing_dates) == 0:
        return missing_dates

    # Calculate list start date to avoid getting data before the
    # user-set start date. Convert both dates/datetimes to date to avoid error
    # from trying to compare different types.
    start_date = max(
        min(missing_dates) - timedelta(days=PAD_DAYS - 1),
        export_start_date.date()
    )

    retrieve_dates = {date.date() for date in pd.date_range(
        start=start_date,
        end=date.today(),
        freq='D')}

    return list(retrieve_dates)


def format_dates_for_query(date_list):
    """Format list of dates as needed for query.

    Date list is turned into a single string for use in
    BigQuery query `where` statement that filters by date.

    Parameters
    ----------
    dates_list: list
        collection of dates of days we want to pull data for

    Returns
    -------
    str: "timestamp("YYYY-MM-DD"), ..."
    """
    earliest_available_symptom_search_year = 2017

    filtered_date_strings = [datetime.strftime(date, "%Y-%m-%d")
                             for date in date_list if date.year >= earliest_available_symptom_search_year]

    # Convert list of dates into list of BigQuery-compatible timestamps.
    query_string = 'timestamp("' + \
        '"), timestamp("'.join(filtered_date_strings) + '")'

    return query_string


def produce_query(level, date_string):
    """Create query string.

    Parameters
    ----------
    level: str
        "county" or "state"
    date_string: str
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
    where timestamp(date) in ({date_list}) and
        country_region_code = "US"
    """
    base_level_table = {"state": "symptom_search_sub_region_1_daily",
                        "county": "symptom_search_sub_region_2_daily"}

    # Add custom values to base_query
    query = base_query.format(
        symptom_cols=", ".join(colname_map.keys()),
        symptom_table=base_level_table[level],
        date_list=date_string)

    return query


def pull_gs_data_one_geolevel(level, date_string):
    """Pull latest data for a single geo level.

    Fetch data and transform it into the appropriate format, as described in
    the preprocess function.

    Note that we retrieve state level data from "symptom_search_sub_region_1_daily"
    where there are state level data for 51 states including 'District of Columbia'.

    We retrieve the county level data from "symptom_search_sub_region_2_daily"
    where there is county level data available except District of Columbia.
    We filter the data such that we only keep rows with valid FIPS.

    Each of these tables should be static and contain all dates.

    PS:  No information for PR

    Parameters
    ----------
    level: str
        "county" or "state"
    date_string: str
        "timestamp("YYYY-MM-DD"), ..." where timestamps are BigQuery-compatible

    Returns
    -------
    pd.DataFrame
    """
    query = produce_query(level, date_string)

    df = pandas_gbq.read_gbq(query, progress_bar_type=None)

    if len(df) == 0:
        df = pd.DataFrame(
            columns=["open_covid_region_code", "date"] +
            list(colname_map.keys())
        )

    df = preprocess(df, level)

    return df


def initialize_credentials(path_to_credentials):
    """Provide pandas_gbq with BigQuery credentials.

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
    retrieve_dates = get_all_dates(receiving_dir, export_start_date)
    retrieve_dates_dict = format_dates_for_query(retrieve_dates)

    initialize_credentials(path_to_credentials)

    # Create dictionary for state and county level data
    dfs = {}

    # For state level data
    dfs["state"] = pull_gs_data_one_geolevel("state", retrieve_dates_dict)

    # For county level data
    dfs["county"] = pull_gs_data_one_geolevel("county", retrieve_dates_dict)

    # Add District of Columbia as county
    try:
        df_dc_county = dfs["state"][dfs["state"]["geo_id"] == "dc"].drop(
            "geo_id", axis=1)
        df_dc_county["geo_id"] = DC_FIPS
        dfs["county"] = dfs["county"].append(df_dc_county)
    except KeyError:
        pass

    return dfs
