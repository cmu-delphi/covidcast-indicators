"""Functions for mapping geographic regions."""

import pandas as pd
from delphi_utils.geomap import GeoMapper


def convert_geo(df: pd.DataFrame, geo: str, gmpr: GeoMapper) -> pd.DataFrame:
    """Map a df to desired regions."""
    if geo == "county":
        output_df = df.copy()
        output_df["geo_id"] = output_df["fips_code"]  # fips is provided in data
    elif geo == "state":
        output_df = df.copy()
        output_df["geo_id"] = output_df["state"]  # state is provided in data
    else:
        output_df = gmpr.add_geocode(df, "zip", geo)
        output_df["geo_id"] = output_df[geo]
    return output_df
