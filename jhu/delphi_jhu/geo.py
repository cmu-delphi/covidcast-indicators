# -*- coding: utf-8 -*-
"""Functions for translating between geocodes."""
import pandas as pd
from delphi_utils import GeoMapper

VALID_GEO_RES = ("county", "state", "msa", "hrr", "hhs", "nation")
INCIDENCE_BASE = 100000

def geo_map(df: pd.DataFrame, geo_res: str):
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
    if geo_res == "county":
        df.rename(columns={'fips': 'geo_id'}, inplace=True)
    elif geo_res == "state":
        df = df.set_index("fips")
        # Zero out the state FIPS population to avoid double counting.
        state_fips_codes = {str(x).zfill(2) + "000" for x in range(1,73)}
        subset_state_fips_codes = set(df.index.values) & state_fips_codes
        df.loc[subset_state_fips_codes, "population"] = 0
        df = df.reset_index()
        df = gmpr.replace_geocode(df, "fips", "state_id", new_col="geo_id", date_col="timestamp")
    else:
        df = gmpr.replace_geocode(df, "fips", geo_res, new_col="geo_id", date_col="timestamp")
    df["incidence"] = df["new_counts"] / df["population"] * INCIDENCE_BASE
    df["cumulative_prop"] = df["cumulative_counts"] / df["population"] * INCIDENCE_BASE
    df['new_counts'] = df['new_counts']
    df['cumulative_counts'] = df['cumulative_counts']
    return df
