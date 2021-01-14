"""Retrieve data and wrangle into appropriate format."""
# -*- coding: utf-8 -*-
import re

import numpy as np
import pandas as pd
from datetime import date, timedelta, datetime
from datetime.datetime import strptime, strftime
from pandas_gbq import read_gbq
from os import listdir
from os.path import isfile, join

from .constants import STATE_TO_ABBREV, DC_FIPS, METRICS, COMBINED_METRIC

base_query =
"""
select
    case
        when sub_region_2_code is null then sub_region_1_code
        when sub_region_2_code is not null then concat(sub_region_1_code, "-", sub_region_2_code)
    end as open_covid_region_code,
    sub_region_2_code,
    date,
    {symptom_cols}
from `bigquery-public-data.covid19_symptom_search.{symptom_table}`
where timestamp(date) in ({date_list})
"""


def get_geo_id(region_code):
    """
    Extract fips code from region code.

    There are region code in the format of "US-state" and "US-state-fips". In
    county level report, we only consider rows with fips info provided.
    """
    splits = region_code.split("-")
    if len(splits) == 3:
        return splits[2]
    return np.nan


def preprocess(df, level):
    """
    Conforms the pulled data from Google COVID-19 Search Trends symptoms data into a dataset.

    The output dataset has:

    - Each row corresponds to (County/State, Date),
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
        df.rename({"symptom:" + metric: metric}, axis=1, inplace=True)
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
            "Part of necessary columns are missed. The dataset "
            "schema may have changed.  Please investigate."
        )

    # Make sure each FIPS/state has same number of rows
    geo_list = df["geo_id"].unique()
    date_list = pd.date_range(start=df["date"].min(),
                              end=df["date"].max(),
                              freq='D')
    index_df = pd.MultiIndex.from_product(
        [geo_list, date_list], names=['geo_id', 'date']
    )
    df = df.set_index(["geo_id", "date"]
                      ).reindex(
        index_df
    ).reset_index(
    ).rename({"date": "timestamp"}, axis=1)

    return df


def get_missing_dates(receiving_dir, start_date):
    """
    """
    OUTPUT_NAME_PATTERN = re.compile("^[0-9]{8}_.*[.]csv")
    existing_output_files = [f for f in listdir(receiving_dir) if isfile(
        join(receiving_dir, f)) and OUTPUT_NAME_PATTERN.match(f)]

    existing_output_dates = {strptime(f[0:8]).date()
                             for f in existing_output_files}
    expected_dates = {
        start_date + timedelta(days=i) for i in range((date.today() - start_date).days + 1)}

    missing_dates = [strftime(d, "%Y-%m-%d")
                     for d in expected_dates.difference(existing_output_dates)]

    # TODO: Drop dates with year < 2017 (Google doesn't provide data that long ago)
    return missing_dates


def pull_gs_data(project_id, api_key, receiving_dir, start_date):
    """Pull latest dataset and transform it into the appropriate format.

    Pull the latest Google COVID-19 Search Trends symptoms dataset, and
    conforms it into a dataset as described in preprocess function.

    Note that we retrieve state level data from "2020_US_daily_symptoms_dataset.csv"
    where there are state level data for 51 states including 'District of Columbia'.

    We retrieve the county level data from "/subregions/state/**daily**.csv"
    where there is county level data available except District of Columbia.
    We filter the data such that we only keep rows with valid FIPS.

    PS:  No information for PR

    Parameters
    ----------
    base_url: str
        Base URL for pulling the Google COVID-19 Search Trends symptoms dataset
    level: str
        "county" or "state"

    Returns
    -------
    dict: {"county": pd.DataFrame, "state": pd.DataFrame}
    """

    base_states_table = "states_daily_{year}"
    base_county_table = "counties_daily_{year}"

    # TODO: Modify pull_gs_data() call to get needed args from params.
    # TODO: Check how other indicators handle api keys.
    # TODO: Make a dict {<tablename with year>: <formatted list of dates to use in where>}. Iterate over the dict to make multiple queries; add dfs to list; concat() together after loop.
    # Fetch and format dates we want to attempt to retrieve
    missing_dates = get_missing_dates(receiving_dir, start_date)
    date_list = 'timestamp("' + '"), timestamp("'.join(missing_dates) + '")'

    # Create map of old to new column names.
    colname_map = {metric.replace(
        " ", "_"): "symptom:" + metric for metric in METRICS}

    # Create dictionary for state and county level data
    dfs = {}

    # For state level data
    state_query = base_query.format(
        symptom_cols=", ".join(colname_map.keys()),
        symptom_table=base_states_table.format(year="2020"),
        date_list=date_list)

    df = read_gbq(state_query, project_id=project_id)
    df["geo_id"] = df["open_covid_region_code"].apply(
        lambda x: x.split("-")[1].lower())
    df.rename(colname_map, axis=1, inplace=True)
    dfs["state"] = preprocess(df, "state")

    # For county level data
    county_query = base_query.format(
        symptom_cols=", ".join(colname_map.keys()),
        symptom_table=base_counties_table.format(year="2020"),
        date_list=date_list)

    df = read_gbq(county_query, project_id=project_id)
    df["geo_id"] = df["open_covid_region_code"].apply(get_geo_id)
    df.rename(colname_map, axis=1, inplace=True)
    dfs["county"] = preprocess(df, "county")

    # Add District of Columbia County
    try:
        df_dc_county = dfs["state"][dfs["state"]["geo_id"] == "dc"].drop(
            "geo_id", axis=1)
        df_dc_county.loc[:, "geo_id"] = DC_FIPS
        dfs["county"] = dfs["county"].append(df_dc_county)
    except KeyError:
        pass

    return dfs
