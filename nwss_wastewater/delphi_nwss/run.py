# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_nwss`.  `run_module`'s lone argument should be a
nested dictionary of parameters loaded from the params.json file.  We expect the `params` to have
the following structure:
    - "common":
        - "export_dir": str, directory to write daily output
        - "log_filename": (optional) str, path to log file
        - "log_exceptions" (optional): bool, whether to log exceptions to file
    - "indicator": (optional)
        - "wip_signal": (optional) Any[str, bool], list of signals that are
            works in progress, or True if all signals in the registry are works
            in progress, or False if only unpublished signals are.  See
            `delphi_utils.add_prefix()`
        - "test_file" (optional): str, name of file from which to read test data
        - "socrata_token": str, authentication for upstream data pull
"""

import time
from datetime import datetime

import numpy as np
from delphi_utils import (
    GeoMapper,
    get_structured_logger,
    create_export_csv,
)
from delphi_utils.nancodes import add_default_nancodes

from .constants import GEOS, METRIC_SIGNALS, PROVIDER_NORMS, SIGNALS
from .pull import pull_nwss_data


def add_needed_columns(df, col_names=None):
    """Short util to add expected columns not found in the dataset."""
    if col_names is None:
        col_names = ["se", "sample_size"]
    else:
        assert "geo_value" not in col_names
        assert "time_value" not in col_names
        assert "value" not in col_names

    for col_name in col_names:
        df[col_name] = np.nan
    df = add_default_nancodes(df)
    return df


def logging(start_time, run_stats, logger):
    """Boilerplate making logs."""
    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    min_max_date = run_stats and min(s[0] for s in run_stats)
    csv_export_count = sum(s[-1] for s in run_stats)
    max_lag_in_days = min_max_date and (datetime.now() - min_max_date).days
    formatted_min_max_date = min_max_date and min_max_date.strftime("%Y-%m-%d")
    logger.info(
        "Completed indicator run",
        elapsed_time_in_seconds=elapsed_time_in_seconds,
        csv_export_count=csv_export_count,
        max_lag_in_days=max_lag_in_days,
        oldest_final_export_date=formatted_min_max_date,
    )


def run_module(params):
    """
    Run the indicator.

    Arguments
    --------
    params:  Dict[str, Any]
        Nested dictionary of parameters.
    """
    start_time = time.time()
    logger = get_structured_logger(
        __name__,
        filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True),
    )
    export_dir = params["common"]["export_dir"]
    socrata_token = params["indicator"]["socrata_token"]
    run_stats = []
    ## build the base version of the signal at the most detailed geo level you can get.
    ## compute stuff here or farm out to another function or file
    df_pull = pull_nwss_data(socrata_token, logger)
    geomapper = GeoMapper()
    # iterate over the providers and the normalizations that they specifically provide
    for provider, normalizations in PROVIDER_NORMS.items():
        for normalization in normalizations:
            # copy by only taking the relevant subsection
            df_prov_norm = df_pull[
                (df_pull.provider == provider)
                & (df_pull.normalization == normalization)
            ]
            df_prov_norm = df_prov_norm.drop(["provider", "normalization"], axis=1)
            for sensor in [*SIGNALS, *METRIC_SIGNALS]:
                full_sensor_name = sensor + "_" + provider + "_" + normalization
                for geo in GEOS:
                    logger.info(
                        "Generating signal and exporting to CSV",
                        metric=full_sensor_name,
                    )
                    if geo == "nation":
                        df_prov_norm["nation"] = "us"
                    agg_df = geomapper.aggregate_by_weighted_sum(
                        df_prov_norm,
                        geo,
                        sensor,
                        "timestamp",
                        "population_served",
                    )
                    agg_df = agg_df.rename(
                        columns={geo: "geo_id", f"weighted_{sensor}": "val"}
                    )
                    # add se, sample_size, and na codes
                    agg_df = add_needed_columns(agg_df)
                    # actual export
                    dates = create_export_csv(
                        agg_df,
                        geo_res=geo,
                        export_dir=export_dir,
                        sensor=full_sensor_name,
                    )
                    if len(dates) > 0:
                        run_stats.append((max(dates), len(dates)))
    ## log this indicator run
    logging(start_time, run_stats, logger)
