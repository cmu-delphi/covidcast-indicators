"""Evaluate data source for problems."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List

import delphi_utils.covidcast_port as covidcast
import pandas as pd

covidcast._ASYNC_CALL = True  # pylint: disable=protected-access

@dataclass
class Complaint:
    """Container for a SirCAL complaint."""

    message: str
    data_source: str
    signal: str
    geo_types: List[str]
    last_updated: pd.Timestamp
    maintainers: List[str]

    def __str__(self):
        """Plain text string form of complaint."""
        return "{source}::{signal} ({geos}) {message}; last updated {updated}".format(
            source=self.data_source, signal=self.signal, geos=", ".join(self.geo_types),
            message=self.message, updated=self.last_updated.strftime("%Y-%m-%d"))

    def to_md(self):
        """Markdown formatted form of complaint."""
        return "*{source}* `{signal}` ({geos}) {message}; last updated {updated}.".format(
            source=self.data_source, signal=self.signal, geos=", ".join(self.geo_types),
            message=self.message, updated=self.last_updated.strftime("%Y-%m-%d"))


def check_source(data_source, meta, params, grace, logger):  # pylint: disable=too-many-locals
    """Iterate over all signals from a source and check for problems.

    Possible problems:

    - Newest available data exceeds max age.
    - Gap between subsequent data points exceeds max gap.

    For example, consider a source with a max age of 5 days and max gap of 1
    day. If today is 2020-10-15, and the latest available data is from
    2020-10-09, the max age is exceeded. If there is no data available on
    2020-10-07, but there is on 2020-10-06 and 2020-10-08, there is a gap of 2
    days and the max gap is exceeded.

    The gap window controls how much data we check for gaps -- a gap window of
    10 days means we check the most recent 10 days of data. Defaults to 7.

    """
    source_config = params[data_source]
    retired_signals = source_config.get("retired-signals")
    gap_window = pd.Timedelta(days=source_config.get("gap_window", 7))
    max_allowed_gap = source_config.get("max_gap", 1)

    signals = meta[(meta.data_source == data_source) &
                   ~meta.apply(_is_retired, axis=1, retired_signals=retired_signals)]

    now = pd.Timestamp.now()

    age_complaints = {}
    gap_complaints = {}

    for _, row in signals.iterrows():
        logger.info("Retrieving signal",
            data_source=data_source,
            signal=row["signal"],
            start_day=(datetime.now() - timedelta(days = 14)).strftime("%Y-%m-%d"),
            end_day=datetime.now().strftime("%Y-%m-%d"),
            geo_type=row["geo_type"])

        latest_data = covidcast.signal(
            data_source, row["signal"],
            start_day=datetime.now() - timedelta(days = 14),
            end_day=datetime.now(),
            geo_type=row["geo_type"]
        )

        current_lag_in_days = (now - row["max_time"]).days
        lag_calculated_from_api = False

        if latest_data is not None:
            unique_dates = [pd.to_datetime(val).date()
                            for val in latest_data["time_value"].unique()]
            current_lag_in_days = (datetime.now().date() - max(unique_dates)).days
            lag_calculated_from_api = True

        logger.info("Signal lag",
                    current_lag_in_days = current_lag_in_days,
                    data_source = data_source,
                    signal = row["signal"],
                    geo_type=row["geo_type"],
                    lag_calculated_from_api = lag_calculated_from_api)

        if current_lag_in_days > source_config["max_age"] + grace:
            if row["signal"] not in age_complaints:
                age_complaints[row["signal"]] = Complaint(
                    f"is {current_lag_in_days} days old",
                    data_source,
                    row["signal"],
                    [row["geo_type"]],
                    row["max_time"],
                    source_config["maintainers"])
            else:
                age_complaints[row["signal"]].geo_types.append(row["geo_type"])

        # Check max gap
        if max_allowed_gap == -1 or latest_data is None:
            # No gap detection for this source
            continue

        # convert numpy datetime values to pandas datetimes and then to
        # datetime.date, so we can work with timedeltas after
        unique_issues = [pd.to_datetime(val).date()
                        for val in latest_data["issue"].unique()]

        gap_days = [(day - prev_day).days
                    for day, prev_day in zip(unique_dates[1:], unique_dates[:-1])]

        # If we only have a single day of data available then gap days will be
        # empty.
        if not gap_days:
            logger.info(
                "Not enough data to calculate gap days.",
                data_source=data_source,
                signal=row["signal"],
                geo_type=row["geo_type"])
            continue

        gap = max(gap_days) - 1
        logger.info("Detecting days with data present",
                    data_source = data_source,
                    signal = row["signal"],
                    geo_type=row["geo_type"],
                    most_recent_dates_with_data = [x.strftime("%Y-%m-%d") for x in unique_dates],
                    gap_days = gap_days,
                    max_gap = gap,
                    issue_dates = [x.strftime("%Y-%m-%d") for x in unique_issues])

        if gap > max_allowed_gap:
            if row["signal"] not in gap_complaints:
                gap_complaints[row["signal"]] = Complaint(
                    "has a {gap}-day gap of missing data in its most recent "
                    "{gap_window} days of data".format(gap=gap, gap_window=gap_window.days),
                    data_source,
                    row["signal"],
                    [row["geo_type"]],
                    datetime.now(),
                    source_config["maintainers"])
            else:
                gap_complaints[row["signal"]].geo_types.append(row["geo_type"])

    return list(age_complaints.values()) + list(gap_complaints.values())


def _is_retired(row, retired_signals):
    """Determine if a row of the metadata belongs to a retired signal according to the params."""
    if not retired_signals:
        return False
    for signal in retired_signals:
        if row["signal"] == signal:
            return True
        if isinstance(signal, list) and {row["signal"], row["geo_type"]}.issubset(set(signal)):
            return True
    return False
