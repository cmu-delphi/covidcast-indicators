"""Functions for mapping geographic regions."""

import pandas as pd
from delphi_utils.geomap import GeoMapper


def convert_geo(df: pd.DataFrame, geo: str, gmpr: GeoMapper) -> pd.DataFrame:
    """
    Map a DataFrame to desired regions.

    The HHS facility level data contains columns for zip, state, and fips. For state and fips, we
    use them as given. For all other geos, we map from zip (the smallest of the regions) to the
    desired geo.

    Parameters
    ----------
    df: pd.DataFrame
        Input DataFrame containing zip, state, and fips columns.
    geo:
        Desired new geographic resolution.
    gmpr:
        GeoMapper object.

    Returns
    -------
    DataFrame containing new geography column `geo_id` in the `geo` resolution.
    """
    if geo == "county":
        output_df = df.copy()
        output_df["geo_id"] = output_df["fips_code"]
    elif geo == "state":
        output_df = df.copy()
        output_df["geo_id"] = output_df["state"]
    elif geo == "hrr":  # use zip for HRR since zips nest within HRR while FIPS split across HRRs.
        output_df = gmpr.add_geocode(df, "zip", geo)
        output_df["geo_id"] = output_df[geo]
    else:
        output_df = gmpr.add_geocode(df, "fips", geo, from_col="fips_code")
        output_df["geo_id"] = output_df[geo]
    return output_df
