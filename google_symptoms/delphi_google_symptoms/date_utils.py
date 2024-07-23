"""utility functions for date parsing."""

from datetime import date, datetime, timedelta
from itertools import product
from typing import Dict, List, Tuple

import covidcast
from delphi_utils.validator.utils import lag_converter
from pandas import to_datetime

from .constants import COMBINED_METRIC, FULL_BKFILL_START_DATE, PAD_DAYS, SMOOTHERS


def generate_patch_dates(params) -> List[Dict[date, List[date]]]:
    """
    Generate date range for chunking backfilled dates.

    Parameters
    ----------
    params

    Returns
    -------
    list of dicts
    """
    issue_date = datetime.strptime(params["patch"]["start_issue"], "%Y-%m-%d")
    end_date = datetime.strptime(params["patch"]["end_issue"], "%Y-%m-%d")

    patch_dates_list = []

    max_expected_lag = lag_converter(params["validation"]["common"].get("max_expected_lag", {"all": 4}))
    global_max_expected_lag = max(list(max_expected_lag.values()))
    num_export_days = params["validation"]["common"].get("span_length", 14) + global_max_expected_lag

    while issue_date <= end_date:
        # start_date gets padded again when executed in run_module
        expected_start_dt = issue_date - timedelta(days=num_export_days - PAD_DAYS + 2)
        daterange = generate_date_range(expected_start_dt, issue_date, num_export_days)
        patch_date = {issue_date: daterange}
        patch_dates_list.append(patch_date)
        issue_date += timedelta(days=1)
    return patch_dates_list


def _generate_candidate_dates(params: Dict, logger) -> Tuple[date, date, int]:
    """
    Generate candidate export_start_date and candidate_export_end_date with possible lag.

    Parameters

    ----------
    params: dictionary parsed from params.json

    Returns
    Tuple[date, date, int]
    -------
    export_start_date and export_end_date and num_export_days
    """
    export_start_date = datetime.strptime(params["indicator"]["export_start_date"], "%Y-%m-%d")

    # If end_date not specified, use current date.
    export_end_date = datetime.strptime(
        params["indicator"].get("export_end_date", datetime.strftime(date.today(), "%Y-%m-%d")), "%Y-%m-%d"
    )

    num_export_days = params["indicator"]["num_export_days"]

    if num_export_days is None:
        # Generate a list of signals we expect to produce
        sensor_names = set(
            "_".join([metric, smoother, "search"]) for metric, smoother in product(COMBINED_METRIC, SMOOTHERS)
        )

        # Fetch metadata to check how recent each signal is
        covidcast.use_api_key(params["indicator"]["api_credentials"])
        metadata = covidcast.metadata()
        # Filter to only those we currently want to produce, ignore any old or deprecated signals
        gs_metadata = metadata[(metadata.data_source == "google-symptoms") & (metadata.signal.isin(sensor_names))]

        if sensor_names.difference(set(gs_metadata.signal)):
            # If any signal not in metadata yet, we need to backfill its full history.
            logger.warning("Signals missing in the epidata; backfilling full history")
            num_export_days = (export_end_date - FULL_BKFILL_START_DATE).days + 1
        else:
            # Calculate number of days based on what's missing from the API and
            # what the validator expects.
            max_expected_lag = lag_converter(params["validation"]["common"].get("max_expected_lag", {"all": 4}))
            global_max_expected_lag = max(list(max_expected_lag.values()))

            latest_date_diff = (datetime.today() - to_datetime(min(gs_metadata.max_time))).days + 1
            expected_date_diff = params["validation"]["common"].get("span_length", 14) + global_max_expected_lag

            if latest_date_diff > expected_date_diff:
                logger.info(f"Missing dates from: {to_datetime(min(gs_metadata.max_time)).date()}")

            num_export_days = expected_date_diff

    if num_export_days == "all":
        num_export_days = (export_end_date - export_start_date).days + 1

    return export_start_date, export_end_date, num_export_days


def generate_date_range(export_start_date: date, export_end_date: date, num_export_days: int) -> List[date]:
    """Produce date range to retrieve data for.

    Calculate start of date range as a static offset from the end date.
    Pad date range by an additional `PAD_DAYS` days before the earliest date to
    produce data for calculating smoothed estimates.

    Parameters
    ----------
    export_start_date: date
        first date to retrieve data for
    export_end_date: date
        last date to retrieve data for
    num_export_days: int
        number of days before end date to export

    Returns
    -------
    List[date, date]
    """
    start_date = max(export_end_date - timedelta(days=num_export_days), export_start_date)

    retrieve_dates = [start_date - timedelta(days=PAD_DAYS - 1), export_end_date]

    return retrieve_dates
