# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_hhs_facilities    `.
"""

# from datetime import date, datetime, timedelta
from itertools import product
from typing import Callable


# from delphi_epidata import Epidata
from delphi_utils import read_params
from delphi_utils.export import create_export_csv
from delphi_utils.geomap import GeoMapper
import numpy as np
import pandas as pd

from .constants import GEO_RESOLUTIONS, SIGNALS


def run_module():
    """Run entire hhs_facilities indicator."""
    params = read_params()
    # raw_df = query_api()  pull data from API
    # below 5 lines are all just for local testing while API is not online yet.
    raw_df = pd.read_csv("delphi_hhs_facilities/sample_data.csv",
                         dtype={"fips_code": "str"},
                         nrows=10000)
    raw_df["timestamp"] = pd.to_datetime(raw_df["collection_week"])
    raw_df["fips"] = raw_df["fips_code"]

    gmpr = GeoMapper()
    for geo, (sig_name, sig_cols, sig_func, sig_offset) in product(GEO_RESOLUTIONS, SIGNALS):
        mapped_df = convert_geo(raw_df, geo, gmpr)
        output_df = process_df(mapped_df, sig_cols, sig_func, sig_offset)
        create_export_csv(output_df, params["export_dir"], geo, sig_name)


def convert_geo(df: pd.DataFrame, geo: str, gmpr: GeoMapper) -> pd.DataFrame:
    """Map a df to desired regions."""
    if geo == "county":
        output_df = df.copy()
        output_df["geo_id"] = output_df["fips"]
    else:
        output_df = gmpr.add_geocode(df, "fips", geo)
        output_df["geo_id"] = output_df[geo]
    return output_df


def process_df(df: pd.DataFrame,
               input_cols: list,
               signal_func: Callable,
               date_offset: int) -> pd.DataFrame:
    """Process a df to get the value of interest."""
    df_cols = [df[i] for i in input_cols]
    df["val"] = signal_func(df_cols)
    df["se"] = np.nan
    df["sample_size"] = np.nan
    df["timestamp"] = df["timestamp"] + pd.Timedelta(days=date_offset)
    return df
