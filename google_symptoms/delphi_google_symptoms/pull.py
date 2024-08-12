"""Retrieve data and wrangle into appropriate format."""
# -*- coding: utf-8 -*-
import re
from datetime import date, datetime  # pylint: disable=unused-import

import numpy as np
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account

from .constants import COMBINED_METRIC, DC_FIPS, DTYPE_CONVERSIONS, METRICS, SYMPTOM_SETS
from .date_utils import generate_query_dates

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
    KEEP_COLUMNS = ["geo_id", "date"] + METRICS + COMBINED_METRIC

    df.rename(colname_map, axis=1, inplace=True)
    df["geo_id"] = df["open_covid_region_code"].apply(
        lambda x: x.split("-")[-1].lower())

    for cb_metric in COMBINED_METRIC:
        df[cb_metric] = 0
        for metric in SYMPTOM_SETS[cb_metric]:
            df[cb_metric] += df[metric].fillna(0).astype(float)
        df[cb_metric] = df[cb_metric]/len(SYMPTOM_SETS[cb_metric])
        df.loc[df[SYMPTOM_SETS[cb_metric]].isnull().all(axis=1), cb_metric] = np.nan

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
        raise ValueError(  # pylint: disable=raise-missing-from
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
        df.date = pd.to_datetime(df.date)
        df = df.set_index(
            ["geo_id", "date"]
        ).reindex(
            index_df
        ).reset_index(
        )

    df = df.rename({"date": "timestamp"}, axis=1)

    return df


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
    list[str]: ["YYYY-MM-DD"), "YYYY-MM-DD"]
    """
    formatted_date_strings = [datetime.strftime(date, "%Y-%m-%d")
                              for date in date_list]
    return formatted_date_strings


def produce_query(level, date_range):
    """Create query string.

    Parameters
    ----------
    level: str
        "county" or "state"
    date_range: list[str]
        ["YYYY-MM-DD"), "YYYY-MM-DD"] where dates are BigQuery-compatible.

    Returns
    -------
    str
    """
    base_query = """
    select
        case
            when coalesce(sub_region_2_code, "") = "" then sub_region_1_code
            when coalesce(sub_region_2_code, "") != "" then concat(sub_region_1_code, "-", sub_region_2_code)
        end as open_covid_region_code,
        date,
        {symptom_cols}
    from `bigquery-public-data.covid19_symptom_search.{symptom_table}`
    where timestamp(date) between timestamp("{start_date}") and timestamp("{end_date}") and
        country_region_code = "US"
    """
    base_level_table = {"state": "symptom_search_sub_region_1_daily",
                        "county": "symptom_search_sub_region_2_daily"}

    # Add custom values to base_query
    query = base_query.format(
        symptom_cols=", ".join(colname_map.keys()),
        symptom_table=base_level_table[level],
        start_date=date_range[0],
        end_date=date_range[1])

    return query


def pull_gs_data_one_geolevel(level, date_range):
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
    date_range: list[str]
        ["YYYY-MM-DD"), "YYYY-MM-DD"] where dates are BigQuery-compatible.

    Returns
    -------
    pd.DataFrame
    """
    query = produce_query(level, date_range)

    df = pandas_gbq.read_gbq(query, progress_bar_type=None, dtypes = DTYPE_CONVERSIONS)
    if len(df) == 0:
        df = pd.DataFrame(
            columns=["open_covid_region_code", "date"] +
            list(colname_map.keys())
        )

    df = preprocess(df, level)

    return df


def initialize_credentials(credentials):
    """Provide pandas_gbq with BigQuery credentials.

    Parameters
    ----------
    credentials: dict
        Dict of BigQuery API credentials from service account json file

    Returns
    -------
    None
    """
    credentials = service_account.Credentials.from_service_account_info(
        credentials)
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = credentials.project_id


def pull_gs_data(credentials, export_start_date, export_end_date, num_export_days, custom_run_flag):
    """Pull latest dataset for each geo level and combine.

    PS:  No information for PR

    Parameters
    ----------
    credentials: dict
        Dict of BigQuery API credentials from service account json file
    level: str
        "county" or "state"
    export_start_date: date
        first date to retrieve data for
    export_end_date: date
        last date to retrieve data for
    num_export_days: int
        number of days before end date to export

    Returns
    -------
    dict: {"county": pd.DataFrame, "state": pd.DataFrame}
    """
    # Fetch and format dates we want to attempt to retrieve
    export_date_range = generate_query_dates(export_start_date, export_end_date, num_export_days, custom_run_flag)
    retrieve_dates = format_dates_for_query(export_date_range)

    initialize_credentials(credentials)

    # Create dictionary for state and county level data
    dfs = {}

    # For state level data
    dfs["state"] = pull_gs_data_one_geolevel("state", retrieve_dates)
    # For county level data
    dfs["county"] = pull_gs_data_one_geolevel("county", retrieve_dates)


    # Add District of Columbia as county
    try:
        df_dc_county = dfs["state"][dfs["state"]["geo_id"] == "dc"].drop(
            "geo_id", axis=1)
        df_dc_county["geo_id"] = DC_FIPS
        dfs["county"] = pd.concat([dfs["county"], df_dc_county])
    except KeyError:
        pass
    return dfs
