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
    unassigned_counties = df[df["fips"].str.endswith("000")].copy()

    df = df[df["fips"].astype(int) % 1000 != 0].copy()
    gmpr = GeoMapper()
    if geo_res == "county":
        if sensor.endswith("_prop"):
            # It is not clear how to calculate the proportion for unallocated
            # cases/deaths, so we exclude them for those sensors.
            df = df.append(unassigned_counties)
        df.rename(columns={"fips": "geo_id"}, inplace=True)
    elif geo_res in ("state", "hhs", "nation"):
        new_geo = "state_id" if geo_res == "state" else geo_res
        df = df.append(unassigned_counties)
        df = gmpr.replace_geocode(df, "fips", new_geo, new_col="geo_id", date_col="timestamp")
        df.drop("population", axis=1, errors="ignore")
        df = gmpr.add_population_column(df, new_geo)
    else:
        df = gmpr.replace_geocode(df, "fips", geo_res, new_col="geo_id", date_col="timestamp")
    df["incidence"] = df["new_counts"] / df["population"] * INCIDENCE_BASE
    df["cumulative_prop"] = df["cumulative_counts"] / df["population"] * INCIDENCE_BASE
    return df
