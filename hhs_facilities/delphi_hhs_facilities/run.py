# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_hhs_facilities    `.
"""

# from datetime import date, datetime, timedelta

# from delphi_epidata import Epidata
from delphi_utils import read_params
from delphi_utils.export import create_export_csv
from delphi_utils.geomap import GeoMapper
import numpy as np
import pandas as pd

from .constants import GEO_RESOLUTIONS


def run_module():
    """Run entire hhs_facilities indicator."""
    params = read_params()
    # raw_df = query_api()  pull data from API
    # below 3 lines are all just for local testing while API is not online yet.
    raw_df = pd.read_csv("delphi_hhs_facilities/sample_data.csv", dtype={"fips_code": "str"})
    raw_df["timestamp"] = pd.to_datetime(raw_df["collection_week"])
    raw_df["fips"] = raw_df["fips_code"]

    gmpr = GeoMapper()
    for geo, signal in GEO_RESOLUTIONS:
        mapped_df = convert_geo(raw_df, geo, gmpr)
        output_df = process_df(mapped_df)
        create_export_csv(output_df, params["export_dir"], geo, "testsignal")


def convert_geo(df: pd.DataFrame, geo: str, gmpr: GeoMapper) -> pd.DataFrame:
    """Map a df to desired regions."""
    if geo == "county":
        output_df = df.copy()
        output_df["geo_id"] = output_df["county"]
    else:
        output_df = gmpr.add_geocode(df, "fips", geo)
        output_df["geo_id"] = output_df[geo]
    return output_df


def process_df(df: pd.DataFrame) -> pd.DataFrame:
    """Process a df to get the value of interest."""
    df["se"] = np.nan
    df["sample_size"] = np.nan
    df["val"] = 1  # Do real processing here
    return df
