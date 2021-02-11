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
    map_df = gmpr._load_crosswalk("fips", geo_res)  # pylint: disable=protected-access
    # Add population as weights
    map_df = gmpr.add_population_column(map_df, "fips")
    if geo_res == "hrr":
        map_df["population"] = map_df["population"] *  map_df["weight"]
    msa_pop = map_df.groupby(geo_res).sum().reset_index()
    map_df = map_df.merge(
            msa_pop, on=geo_res, how="inner", suffixes=["_raw", "_groupsum"]
            )
    map_df["weight"] = map_df["population_raw"] / map_df["population_groupsum"]

    map_df = pd.pivot_table(
                 map_df, values='weight', index=["fips"], columns=[geo_res]
              ).fillna(0).reset_index().rename({"fips": "geo_id"}, axis = 1)
    return map_df

def geo_map(df, geo_res):
    """
    Compute derived HRR and MSA counts as a weighted sum of the county dataset.

    Parameters
    ----------
    df: pd.DataFrame
        a data frame with columns "geo_id", "timestamp",
        and columns for signal vals
    geo_res: str
        "msa" or "hrr"

    Returns
    -------
    pd.DataFrame
        A dataframe with columns "geo_id", "timestamp",
        and columns for signal vals.
        The geo_id has been converted from fips to HRRs/MSAs
    """
    if geo_res in set(["county", "state"]):
        return df

    map_df = generate_transition_matrix(geo_res)
    converted_df = pd.DataFrame(columns = df.columns)
    for _date in df["timestamp"].unique():
        val_lists = df[df["timestamp"] == _date].merge(
                map_df["geo_id"], how="right"
                )[METRICS + [COMBINED_METRIC]].fillna(0)
        newdf = pd.DataFrame(
                np.matmul(map_df.values[:, 1:].T, val_lists.values),
                columns = list(val_lists.keys())
                )
        newdf["timestamp"] = _date
        newdf["geo_id"] = list(map_df.keys())[1:]
        mask = (newdf == 0)
        newdf[mask] = np.nan
        converted_df = converted_df.append(newdf)
    return converted_df
