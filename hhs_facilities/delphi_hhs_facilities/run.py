# -*- coding: utf-8 -*-
"""Main function to run the HHS facilities module. Run with `python -m delphi_hhs_facilities`."""
from datetime import datetime
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
    stats = []
    for geo, (sig_name, sig_cols, sig_func, sig_offset) in product(GEO_RESOLUTIONS, SIGNALS):
        logger.info("Generating signal and exporting to CSV",
                    geo_res = geo,
                    signal_name = sig_name)
        mapped_df = convert_geo(filled_fips_df, geo, gmpr)
        output_df = generate_signal(mapped_df, sig_cols, sig_func, sig_offset)
        dates = create_export_csv(output_df, params["common"]["export_dir"], geo, sig_name)
        if len(dates) > 0:
            stats.append((max(dates), len(dates)))

    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    min_max_date = stats and min(s[0] for s in stats)
    csv_export_count = sum(s[-1] for s in stats)
    max_lag_in_days = min_max_date and (datetime.now() - min_max_date).days
    formatted_min_max_date = min_max_date and min_max_date.strftime("%Y-%m-%d")
    logger.info("Completed indicator run",
                elapsed_time_in_seconds = elapsed_time_in_seconds,
                csv_export_count = csv_export_count,
                max_lag_in_days = max_lag_in_days,
                oldest_final_export_date = formatted_min_max_date)
