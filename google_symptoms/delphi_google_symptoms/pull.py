"""Retrieve data and wrangle into appropriate format."""
# -*- coding: utf-8 -*-
import re

import numpy as np
import pandas as pd

from .constants import STATE_TO_ABBREV, DC_FIPS, METRICS, COMBINED_METRIC

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
        df.rename({"symptom:" + metric: metric}, axis = 1, inplace = True)
        df[COMBINED_METRIC] += df[metric].fillna(0)
    df.loc[
            (df["Anosmia"].isnull())
            & (df["Ageusia"].isnull())
            , COMBINED_METRIC] = np.nan

    # Delete rows with missing FIPS
    null_mask = (df["geo_id"].isnull())
    df = df.loc[~null_mask]

    # Confirm FIPS
    if level == "county":
        test_result = []
        for fips in df["geo_id"].values:
            test_result.append(len(re.findall(r"\b\d{5}\b",fips)) == 1)
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
        ).rename({"date": "timestamp"}, axis = 1)

    return df

def pull_gs_data(base_url):
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
    # Create dictionary for state and county level data
    dfs = {}
    # For state level data
    df = pd.read_csv(base_url.format(sub_url="/", state=""),
                     parse_dates = ["date"])
    df["geo_id"] = df["open_covid_region_code"].apply(
            lambda x: x.split("-")[1].lower())
    dfs["state"] = preprocess(df, "state")

    # For county level data
    dfs["county"] = pd.DataFrame(columns = dfs["state"].columns)
    for state in list(STATE_TO_ABBREV.keys()):
        sub_url = "/subregions/" + "%20".join(state.split("_")) + "/"
        df = pd.read_csv(base_url.format(sub_url=sub_url,
                                         state=state+"_"),
                         parse_dates = ["date"])
        df["geo_id"] = df["open_covid_region_code"].apply(get_geo_id)
        dfs["county"] = dfs["county"].append(preprocess(df, "county"))

    # Add District of Columbia County
    try:
        df_dc_county = dfs["state"][dfs["state"]["geo_id"]=="dc"].drop(
                "geo_id", axis = 1)
        df_dc_county.loc[:, "geo_id"] = DC_FIPS
        dfs["county"] = dfs["county"].append(df_dc_county)
    except KeyError:
        pass

    return dfs
