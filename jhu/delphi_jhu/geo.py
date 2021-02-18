# -*- coding: utf-8 -*-
"""Functions for translating between geocodes."""
import pandas as pd
from delphi_utils import GeoMapper

VALID_GEO_RES = ("county", "state", "msa", "hrr", "hhs", "nation")
INCIDENCE_BASE = 100000


def geo_map(df: pd.DataFrame, geo_res: str, sensor: str):
    """
    Map and aggregate a DataFrame at the county resolution to the geographic resolution geo_res.

    Parameters
    ----------
    df: pd.DataFrame
        Columns: fips, timestamp, new_counts, cumulative_counts, population ...
    geo_res: str
        Geographic resolution to which to aggregate.  Valid options:
        ('fips', 'state', 'msa', 'hrr', 'hhs', 'nation').
    sensor: str
        sensor type. Valid options:
        ("new_counts", "cumulative_counts",
        "incidence", "cumulative_prop")

    Returns
    -------
    pd.DataFrame
        Columns: geo_id, timestamp, ...
    """
    df = df.copy()
    if geo_res not in VALID_GEO_RES:
        raise ValueError(f"geo_res must be one of {VALID_GEO_RES}")
    gmpr = GeoMapper()
    df = add_county_pop(df, gmpr)
    unassigned_counties = df[df["fips"].str.endswith("000")].copy()
    df = df[~df["fips"].str.endswith("000")].copy()
    if geo_res == "county":
        if not sensor in ("incidence",  "cumulative_prop"): # prop signals
            # It is not clear how to calculate the proportion for unallocated
            # cases/deaths, so we exclude them for those sensors.
            df = df.append(unassigned_counties) if not unassigned_counties.empty else df
        df.rename(columns={"fips": "geo_id"}, inplace=True)
    elif geo_res in ("state", "hhs", "nation"):
        geo = "state_id" if geo_res == "state" else geo_res
        df = df.append(unassigned_counties) if not unassigned_counties.empty else df
        df = gmpr.replace_geocode(df, "fips", geo, new_col="geo_id", date_col="timestamp")
    else:
        df = gmpr.replace_geocode(df, "fips", geo_res, new_col="geo_id", date_col="timestamp")
    df["incidence"] = df["new_counts"] / df["population"] * INCIDENCE_BASE
    df["cumulative_prop"] = df["cumulative_counts"] / df["population"] * INCIDENCE_BASE
    return df


def add_county_pop(df: pd.DataFrame, gmpr: GeoMapper):
    """
    Add county populations to the data with special US territory handling.

    Since Guam, Northern Mariana Islands, American Samoa, and the Virgin Islands are reported as
    megafips instead of actual counties in JHU, they would normally not have a population added.
    In addition to adding populations for the non-territory counties, this function adds in
    the entire territory's population for the 4 aforementioned regions.

    Parameters
    ----------
    df
        DataFrame with county level information and county column named "fips"
    gmpr
        GeoMapper

    Returns
    -------
        Dataframe with added population column
    """
    is_territory_mega = df.fips.isin(["78000", "69000", "66000", "60000"])
    territories = df[is_territory_mega]
    territories_state_id = gmpr.add_geocode(territories, "fips", "state_id")
    territories_pop = gmpr.add_population_column(territories_state_id, "state_id", dropna=False)
    territories_pop.drop("state_id", axis=1, inplace=True)
    nonterritories = df[~is_territory_mega]
    nonterritories_pop = gmpr.add_population_column(nonterritories, "fips", dropna=False)
    return pd.concat([nonterritories_pop, territories_pop], ignore_index=True)
