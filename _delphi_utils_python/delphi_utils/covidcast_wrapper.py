"""module for covidcast api call wrapper."""

from datetime import date, timedelta
from typing import Iterable, Union

import pandas as pd
from delphi_epidata import Epidata
from epiweeks import Week


def date_generator(startdate: date, enddate: date, time_type: str) -> Iterable[date]:
    """
    Take start date and end date and generates date string.

    Parameters
    ----------
    startdate: date
    enddate: date
    time_type: str

    Returns
    -------
    generator of str
    """
    if time_type.lower() == "day":
        while startdate <= enddate:
            yield startdate.strftime("%Y-%m-%d")
            startdate = startdate + timedelta(days=1)
    elif time_type.lower() == "week":
        while startdate <= enddate:
            epiweek = Week.fromdate(startdate)
            yield epiweek
            startdate = startdate + timedelta(days=7)


def _parse_datetimes(date_int: int, time_type: str, date_format: str = "%Y%m%d") -> Union[pd.Timestamp, None]:
    """Convert a date or epiweeks string into timestamp objects.

    Datetimes (length 8) are converted to their corresponding date, while epiweeks (length 6)
    are converted to the date of the start of the week. Returns nan otherwise

    Epiweeks use the CDC format.

    date_int: Int representation of date.
    time_type: The temporal resolution to request this data. Most signals
      are available at the "day" resolution (the default); some are only
      available at the "week" resolution, representing an MMWR week ("epiweek").
    date_format: String of the date format to parse.
    :returns: Timestamp.
    """
    date_str = str(date_int)
    if time_type == "day":
        return pd.to_datetime(date_str, format=date_format)
    if time_type == "week":
        epiwk = Week(int(date_str[:4]), int(date_str[-2:]))
        return pd.to_datetime(epiwk.startdate())
    return None


def metadata() -> Union[pd.DataFrame, None]:
    """
    Make covidcast metadata api call.

    Returns
    -------
    pd.DataFrame of covidcast metadata.
    """
    response = Epidata.covidcast_meta()

    if response["result"] != 1:
        # Something failed in the API and we did not get real metadata
        raise RuntimeError("Error when fetching metadata from the API", response["message"])

    df = pd.DataFrame.from_dict(response["epidata"])
    df["min_time"] = df.apply(lambda x: _parse_datetimes(x.min_time, x.time_type), axis=1)
    df["max_time"] = df.apply(lambda x: _parse_datetimes(x.max_time, x.time_type), axis=1)
    df["last_update"] = pd.to_datetime(df["last_update"], unit="s")
    return df


def signal(
    data_source: str,
    signal: str,  # pylint: disable=W0621
    start_day: date = None,
    end_day: date = None,
    geo_type: str = "county",
    geo_values: Union[str, Iterable[str]] = "*",
    as_of: date = None,
    lag: int = None,
    time_type: str = "day",
) -> Union[pd.DataFrame, None]:
    """
    Makes covidcast signal api call.

    data_source: String identifying the data source to query, such as
      ``"fb-survey"``.
    signal: String identifying the signal from that source to query,
      such as ``"smoothed_cli"``.
    start_day: Query data beginning on this date. Provided as a
      ``datetime.date`` object. If ``start_day`` is ``None``, defaults to the
      first day data is available for this signal. If ``time_type == "week"``, then
      this is rounded to the epiweek containing the day (i.e. the previous Sunday).
    end_day: Query data up to this date, inclusive. Provided as a
      ``datetime.date`` object. If ``end_day`` is ``None``, defaults to the most
      recent day data is available for this signal. If ``time_type == "week"``, then
      this is rounded to the epiweek containing the day (i.e. the previous Sunday).
    geo_type: The geography type for which to request this data, such as
      ``"county"`` or ``"state"``. Available types are described in the
      COVIDcast signal documentation. Defaults to ``"county"``.
    geo_values: The geographies to fetch data for. The default, ``"*"``,
      fetches all geographies. To fetch one geography, specify its ID as a
      string; multiple geographies can be provided as an iterable (list, tuple,
      ...) of strings.
    as_of: Fetch only data that was available on or before this date,
      provided as a ``datetime.date`` object. If ``None``, the default, return
      the most recent available data. If ``time_type == "week"``, then
      this is rounded to the epiweek containing the day (i.e. the previous Sunday).
    lag: Integer. If, for example, ``lag=3``, fetch only data that was
      published or updated exactly 3 days after the date. For example, a row
      with ``time_value`` of June 3 will only be included in the results if its
      data was issued or updated on June 6. If ``None``, the default, return the
      most recently issued data regardless of its lag.
    time_type: The temporal resolution to request this data. Most signals
      are available at the "day" resolution (the default); some are only
      available at the "week" resolution, representing an MMWR week ("epiweek").
    :returns: A Pandas data frame with matching data, or ``None`` if no data is
      returned. Each row is one observation on one day in one geographic location.
      Contains the following columns:
    """
    if start_day > end_day:
        raise ValueError(
            "end_day must be on or after start_day, but " f"start_day = '{start_day}', end_day = '{end_day}'"
        )

    time_values = list(date_generator(start_day, end_day))

    response = Epidata.covidcast(
        data_source,
        signal,
        time_type=time_type,
        geo_type=geo_type,
        time_values=time_values,
        geo_value=geo_values,
        as_of=as_of,
        lag=lag,
    )
    if response["result"] != 1:
        # Something failed in the API and we did not get real metadata
        raise RuntimeError("Error when fetching signal data from the API", response["message"])

    api_df = pd.DataFrame.from_dict(response["epidata"])
    api_df["issue"] = pd.to_datetime(api_df["issue"], format="%Y%m%d")
    api_df["time_value"] = pd.to_datetime(api_df["time_value"], format="%Y%m%d")
    api_df.drop("direction", axis=1, inplace=True)
    api_df["data_source"] = data_source
    api_df["signal"] = signal

    return api_df
