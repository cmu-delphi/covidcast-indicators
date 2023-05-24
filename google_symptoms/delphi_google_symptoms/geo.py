"""Functions for mapping between geo regions."""
# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
from delphi_utils import GeoMapper
from .constants import METRICS, COMBINED_METRIC

gmpr = GeoMapper()

def generate_transition_matrix(geo_res):
    """
    Generate transition matrix from county to msa/hrr.

    Parameters
    ----------
    geo_res: str
        "msa" or "hrr"

    Returns
    -------
    pd.DataFrame
        columns "geo_id", "timestamp", and "val".
        The first is a data frame for HRR regions and the second are MSA
        regions.
    """
    if geo_res in ["hrr", "msa"]:
        mapping_flag = "fips"
        map_df = gmpr.get_crosswalk("fips", geo_res)
        # Add population as weights
        map_df = gmpr.add_population_column(map_df, "fips")
    else:
        mapping_flag = "state_id"
        map_df = gmpr.get_crosswalk("state", "state")
        map_df = gmpr.add_geocode(map_df, "state_code", geo_res)
        map_df = gmpr.add_population_column(map_df, "state_code")

    if geo_res == "hrr":
        map_df["population"] = map_df["population"] *  map_df["weight"]

    aggregated_pop = map_df.groupby(geo_res).sum(numeric_only=True).reset_index()
    map_df = map_df.merge(
            aggregated_pop, on=geo_res, how="inner", suffixes=["_raw", "_groupsum"]
            )
    map_df["weight"] = map_df["population_raw"] / map_df["population_groupsum"]

    map_df = pd.pivot_table(
                 map_df, values='weight', index=[mapping_flag], columns=[geo_res]
              ).fillna(0).reset_index().rename({mapping_flag: "geo_id"}, axis = 1)
    return map_df

def geo_map(df, geo_res, namescols =  None):
    """
    Compute derived HRR and MSA counts as a weighted sum of the county dataset.

    Parameters
    ----------
    df: pd.DataFrame
        a data frame with columns "geo_id", "timestamp",
        and columns for signal vals
    geo_res: str
        "msa", "hrr", "hhs" or "nation"
    namescols: list of strings
        names of columns of df but geo_id and timestamp
        when running the pipeline, this will always be METRICS+COMBINED_METRIC
        this parameter was added to allow us to run unit tests in subsets of
        metrics and combined_metric's

    Returns
    -------
    pd.DataFrame
        A dataframe with columns "geo_id", "timestamp",
        and columns for signal vals.
        The geo_id has been converted from fips to HRRs/MSAs
    """
    if namescols is None:
        namescols = METRICS + COMBINED_METRIC

    if geo_res == "county":
        return df

    map_df = generate_transition_matrix(geo_res)

    dates_list = df["timestamp"].unique()
    dfs_list = [pd.DataFrame()] * len(dates_list)

    for i, _date in enumerate(dates_list):
        val_lists = df[df["timestamp"] == _date].merge(
                map_df["geo_id"], how="right"
                )[namescols].fillna(0)
        newdf = pd.DataFrame(
                np.matmul(map_df.values[:, 1:].T, val_lists.values),
                columns = list(val_lists.keys())
                )
        newdf["timestamp"] = _date
        newdf["geo_id"] = list(map_df.keys())[1:]
        mask = (newdf == 0)
        newdf[mask] = np.nan
        dfs_list[i] = newdf

    # Reindex to make sure output has same columns as input df. Filled with
    # NaN values if column doesn't already exist.
    return pd.concat(dfs_list).reindex(df.columns, axis=1)
