"""utility functions for date parsing."""

from datetime import date, datetime, timedelta
from itertools import product
from typing import Dict, List, Tuple

import covidcast
from delphi_utils.validator.utils import lag_converter
from pandas import to_datetime

from .constants import COMBINED_METRIC, FULL_BKFILL_START_DATE, PAD_DAYS, SMOOTHERS


def generate_patch_dates(params) -> Dict[date, Tuple[date]]:
    """
    Generate date range for chunking backfilled dates.

    Parameters
    ----------
    params

    Returns
    -------
    dict of date and tuple of date
    """
    issue_date = datetime.strptime(params["patch"]["start_issue"], "%Y-%m-%d")
    end_date = datetime.strptime(params["patch"]["end_issue"], "%Y-%m-%d")
    num_export_days = _generate_num_export_days(params)

    patch_dates = dict()
    while issue_date <= end_date:
        # negate the subtraction done within generate_query_dates
        expected_start_dt = issue_date - timedelta(days=num_export_days - PAD_DAYS + 1)
        daterange = generate_query_dates(expected_start_dt, issue_date, num_export_days, True)
        patch_dates[issue_date] = tuple(daterange)
        issue_date += timedelta(days=1)
    return patch_dates


def _generate_num_export_days(params: Dict) -> int:
    """
    Generate dates for exporting with possible lag.

    Parameters
    ----------
    params: dictionary parsed from params.json

    Returns
    -------
    number of export days
    """
    # Calculate number of days based on what's missing from the API and
    # what the validator expects.
    max_expected_lag = lag_converter(params["validation"]["common"].get("max_expected_lag", {"all": 4}))
    global_max_expected_lag = max(list(max_expected_lag.values()))
    num_export_days = params["validation"]["common"].get("span_length", 14) + global_max_expected_lag
    return num_export_days

def generate_num_export_days(params: Dict, logger) -> [int]:
    """
    Generate dates for exporting based on current available data.

    Parameters

    ----------
    params: dictionary parsed from params.json

    Returns
    -------
    num_export_days: int
    """
    # If end_date not specified, use current date.
    export_end_date = datetime.strptime(
        params["indicator"].get("export_end_date", datetime.strftime(date.today(), "%Y-%m-%d")), "%Y-%m-%d"
    )

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
        latest_date_diff = (datetime.today() - to_datetime(min(gs_metadata.max_time))).days + 1
        expected_date_diff = _generate_num_export_days(params)

        if latest_date_diff > expected_date_diff:
            logger.info(f"Missing dates from: {to_datetime(min(gs_metadata.max_time)).date()}")

        num_export_days = expected_date_diff

    return num_export_days


def generate_query_dates(
    export_start_date: date, export_end_date: date, num_export_days: int, patch_flag: bool
) -> List[date]:
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
    patch_flag: bool
        flag to indicate if the date should be taken from export or calculated based on if it's a patch or regular run

    Returns
    -------
    List[date, date]
    """
    start_date = export_start_date
    if not patch_flag:
        start_date = export_end_date - timedelta(days=num_export_days)
    retrieve_dates = [start_date - timedelta(days=PAD_DAYS - 1), export_end_date]

    return retrieve_dates
