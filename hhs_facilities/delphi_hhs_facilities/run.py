# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_hhs_facilities    `.
"""

from itertools import product

# from delphi_epidata import Epidata
from delphi_utils import read_params
from delphi_utils.export import create_export_csv
from delphi_utils.geomap import GeoMapper
import numpy as np
import pandas as pd

from .constants import GEO_RESOLUTIONS, SIGNALS, NAN_VALUE
from .generate_signals import generate_signal
from .geo import convert_geo


def run_module() -> None:
    """Run entire hhs_facilities indicator."""
    params = read_params()

    # below 2 commands are all just for local testing while API is not online yet.
    raw_df = pd.read_csv("delphi_hhs_facilities/sample_data.csv",
                         dtype={"fips_code": "str"},
                         nrows=1000,
                         na_values=NAN_VALUE)
    raw_df["timestamp"] = pd.to_datetime(raw_df["collection_week"])

    gmpr = GeoMapper()
    for geo, (sig_name, sig_cols, sig_func, sig_offset) in product(GEO_RESOLUTIONS, SIGNALS):
        mapped_df = convert_geo(raw_df, geo, gmpr)
        output_df = generate_signal(mapped_df, sig_cols, sig_func, sig_offset)
        create_export_csv(output_df, params["export_dir"], geo, sig_name)
