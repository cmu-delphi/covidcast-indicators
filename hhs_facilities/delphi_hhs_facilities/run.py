# -*- coding: utf-8 -*-
"""Main function to run the HHS facilities module. Run with `python -m delphi_hhs_facilities`."""

import time

from itertools import product

from delphi_utils.export import create_export_csv
from delphi_utils.geomap import GeoMapper
from delphi_utils import get_structured_logger

from .constants import GEO_RESOLUTIONS, SIGNALS
from .generate_signals import generate_signal
from .geo import convert_geo, fill_missing_fips
from .pull import pull_data


def run_module(params) -> None:
    """
    Run entire hhs_facilities indicator.

    Parameters
    ----------
    params
        Dictionary containing indicator configuration. Expected to have the following structure:
        - "common":
            - "export_dir": str, directory to write output
    """
    start_time = time.time()
    logger = get_structured_logger(
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))

    raw_df = pull_data()
    gmpr = GeoMapper()
    filled_fips_df = fill_missing_fips(raw_df, gmpr)
    for geo, (sig_name, sig_cols, sig_func, sig_offset) in product(GEO_RESOLUTIONS, SIGNALS):
        mapped_df = convert_geo(filled_fips_df, geo, gmpr)
        output_df = generate_signal(mapped_df, sig_cols, sig_func, sig_offset)
        create_export_csv(output_df, params["common"]["export_dir"], geo, sig_name)

    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    logger.info("Completed indicator run",
                elapsed_time_in_seconds=elapsed_time_in_seconds)
