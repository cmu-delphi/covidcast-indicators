# -*- coding: utf-8 -*-
import pandas as pd
from delphi_utils import GeoMapper

INCIDENCE_BASE = 100000

def geo_map(df: pd.DataFrame, geo_res: str):
    """
    Maps a DataFrame df, which contains data at the county resolution, and
    aggregate it to the geographic resolution geo_res.

    Parameters
    ----------
    df: pd.DataFrame
        Columns: fips, timestamp, new_counts, cumulative_counts, population ...
    geo_res: str
        Geographic resolution to which to aggregate.  Valid options:
        ('county', 'state', 'msa', 'hrr').
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
    VALID_GEO_RES = ("county", "state", "msa", "hrr")
    if geo_res not in VALID_GEO_RES:
        raise ValueError(f"geo_res must be one of {VALID_GEO_RES}")

    gmpr = GeoMapper()
    if geo_res == "state":
        df = gmpr.county_to_state(df, fips_col="fips", state_id_col="geo_id", date_col="timestamp")
    elif geo_res == "msa":
        df = gmpr.county_to_msa(df, fips_col="fips", msa_col="geo_id", date_col="timestamp")
        df['geo_id'] = df['geo_id'].astype(int)
        print(df[df['population'] == 0])
    elif geo_res == 'hrr':
        df = gmpr.county_to_hrr(df, fips_col="fips", hrr_col="geo_id", date_col="timestamp")
        df['geo_id'] = df['geo_id'].astype(int)
    elif geo_res == 'county':
        df.rename(columns={'fips': 'geo_id'}, inplace=True)
    df["incidence"] = df["new_counts"] / df["population"] * INCIDENCE_BASE
    df["cumulative_prop"] = df["cumulative_counts"] / df["population"] * INCIDENCE_BASE
    df['new_counts'] = df['new_counts']
    df['cumulative_counts'] = df['cumulative_counts']
    return df
