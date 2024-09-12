"""utility functions for date parsing."""

from datetime import date, datetime, timedelta
from itertools import product
from typing import Dict, List, Union

import covidcast
from delphi_utils.validator.utils import lag_converter
from pandas import to_datetime

from .constants import COMBINED_METRIC, FULL_BKFILL_START_DATE, PAD_DAYS, SMOOTHERS


def generate_patch_dates(params: Dict) -> Dict[date, Dict[str, Union[date, int]]]:
    """
    Generate date range for chunking backfilled dates.

    Parameters
    ----------
    params: dictionary parsed from params.json

    Returns
    -------
    dict(date: dict(export date range settings))
    """
    issue_date = datetime.strptime(params["patch"]["start_issue"], "%Y-%m-%d")
    end_date = datetime.strptime(params["patch"]["end_issue"], "%Y-%m-%d")
    num_export_days = params["validation"]["common"].get("span_length", 14)

    patch_dates = dict()
    while issue_date <= end_date:
        global_max_expected_lag = get_max_lag(params)
        export_end_date = issue_date - timedelta(days=global_max_expected_lag + 1)
        export_start_date = issue_date - timedelta(days=num_export_days + global_max_expected_lag + 1)

        patch_dates[issue_date] = {
            "export_start_date": export_start_date,
            "export_end_date": export_end_date,
            "num_export_days": num_export_days,
        }

        issue_date += timedelta(days=1)

    return patch_dates


def get_max_lag(params: Dict) -> int:
    """Determine reporting lag for data source."""
    max_expected_lag = lag_converter(params["validation"]["common"].get("max_expected_lag", {"all": 4}))
    return max(list(max_expected_lag.values()))


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

    num_export_days = params["indicator"]["num_export_days"]
    custom_run = False if not params["common"].get("custom_run") else params["common"].get("custom_run", False)

    if num_export_days is None and not custom_run:
        # Fetch metadata to check how recent each signal is
        covidcast.use_api_key(params["indicator"]["api_credentials"])
        metadata = covidcast.metadata()
        # Filter to only those signals we currently want to produce for `google-symptoms`
        gs_metadata = metadata[(metadata.data_source == "google-symptoms") & (metadata.signal.isin(sensor_names))]

        if sensor_names.difference(set(gs_metadata.signal)):
            # If any signal not in metadata yet, we need to backfill its full history.
            logger.warning("Signals missing in the epidata; backfilling full history")
            num_export_days = (export_end_date - FULL_BKFILL_START_DATE).days + 1
        else:
            latest_date_diff = (datetime.today() - to_datetime(min(gs_metadata.max_time))).days + 1

            expected_date_diff = params["validation"]["common"].get("span_length", 14)

            # there's an expected lag of 4 days behind if running from today
            if export_end_date.date() == datetime.today().date():
                global_max_expected_lag = get_max_lag(params)
                expected_date_diff += global_max_expected_lag

            if latest_date_diff > expected_date_diff:
                logger.info(f"Missing dates from: {to_datetime(min(gs_metadata.max_time)).date()}")

            num_export_days = expected_date_diff

    return num_export_days


def generate_query_dates(
    export_start_date: date, export_end_date: date, num_export_days: int, custom_run_flag: bool
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
    custom_run_flag: bool
        flag to indicate if the date should be taken from export or calculated based on if it's a patch or regular run

    Returns
    -------
    List[date, date]
    """
    start_date = export_start_date
    if not custom_run_flag:
        start_date = export_end_date - timedelta(days=num_export_days)
    retrieve_dates = [start_date - timedelta(days=PAD_DAYS - 1), export_end_date]

    return retrieve_dates
