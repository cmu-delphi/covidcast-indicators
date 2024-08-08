"""
script to check converting covidcast api calls with Epidata.covidcast Epidata.covidcast_meta
"""
import time
from collections import defaultdict
from pathlib import Path
from typing import Union, Iterable, Tuple, List, Dict
from datetime import datetime, timedelta, date

import numpy as np
import pandas as pd
import covidcast
from delphi_epidata import Epidata
from pandas.testing import assert_frame_equal
import os
from epiweeks import Week

API_KEY = os.environ.get('DELPHI_API_KEY')
covidcast.use_api_key(API_KEY)

Epidata.debug = True
Epidata.auth = ("epidata", API_KEY)

CURRENT_DIR = Path(__file__).parent

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


def ported_metadata() -> Union[pd.DataFrame, None]:
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


def ported_signal(
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

    if time_type == "day":
        time_values = Epidata.range(start_day.strftime("%Y%m%d"), end_day.strftime("%Y%m%d"))
    else:
        time_values = Epidata.range(start_day.strftime("%Y%W"), end_day.strftime("%Y%W"))
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
        print(f"check {data_source} {signal}")
        # Something failed in the API and we did not get real metadata
        # raise RuntimeError("Error when fetching signal data from the API", response["message"])

    api_df = pd.DataFrame.from_dict(response["epidata"])
    if not api_df.empty:
        api_df["issue"] = api_df.apply(lambda x: _parse_datetimes(x.issue, x.time_type), axis=1)
        api_df["time_value"] = api_df.apply(lambda x: _parse_datetimes(x.time_value, x.time_type), axis=1)
        api_df.drop("direction", axis=1, inplace=True)
        api_df["data_source"] = data_source
        api_df["signal"] = signal

    return api_df

def check_metadata():

    expected_df = covidcast.metadata()
    df = ported_metadata()
    assert_frame_equal(expected_df, df)

def ported_signal(
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

    if time_type == "day":
        time_values = Epidata.range(start_day.strftime("%Y%m%d"), end_day.strftime("%Y%m%d"))
    else:
        time_values = Epidata.range(start_day.strftime("%Y%W"), end_day.strftime("%Y%W"))
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
        print(f"check {data_source} {signal}")
        # Something failed in the API and we did not get real metadata
        # raise RuntimeError("Error when fetching signal data from the API", response["message"])

    api_df = pd.DataFrame.from_dict(response["epidata"])
    if not api_df.empty:
        api_df["issue"] = api_df.apply(lambda x: _parse_datetimes(x.issue, x.time_type), axis=1)
        api_df["time_value"] = api_df.apply(lambda x: _parse_datetimes(x.time_value, x.time_type), axis=1)
        api_df.drop("direction", axis=1, inplace=True)
        api_df["data_source"] = data_source
        api_df["signal"] = signal

    return api_df


def generate_start_date_per_signal() -> Dict[Tuple[datetime, datetime, str], List[Tuple[str]]]:
    """
    Generate a dictionary of date range associated with individual signals


    :return: Dictionary of date ranges to list of data source, signal tuple

    Dict[Tuple[datetime.datetime, datetime.datetime, str],[List[Tuple[str, str]]]
    """
    meta_df = pd.DataFrame.from_dict(Epidata.covidcast_meta()["epidata"])
    meta_df["min_time"] = meta_df["min_time"].astype('str')
    signal_timeframe_dict = defaultdict(list)

    for start_str, data in meta_df.groupby("min_time"):

        data_source_groups = data.groupby("data_source")
        for data_source, df in data_source_groups:
            signals = list(df["signal"].unique())
            time_type = df["time_type"].values[0]
            for signal in signals:
                if time_type == "day":
                    start_time = datetime.strptime(start_str, "%Y%m%d")
                    # explicit start date for google symptom does not match what's in the metadata
                    if data_source == "google-symptoms":
                        start_time = datetime(year=2020, month=2, day=20)
                    end_time = start_time + timedelta(days=30)
                    date_range = (start_time, end_time, time_type)
                    signal_timeframe_dict[date_range].append((data_source, signal))

                elif time_type == "week":
                    start_time = Week(year=int(start_str[:4]), week=int(start_str[-2:]))
                    end_time = (start_time + 2).startdate()
                    date_range = (start_time.startdate(),
                                  end_time, time_type)
                    signal_timeframe_dict[date_range].append((data_source, signal))

    return signal_timeframe_dict


def check_signal():
    """
    Compares the result from covidcast api with Epidata.covidcast

    :return:
    """
    signal_timeframe_dict = generate_start_date_per_signal()
    signal_df_dict = dict()
    for date_range, data_source_signal_list in signal_timeframe_dict.items():
        for data_source, signal in data_source_signal_list:
            time_type = date_range[2]
            expected_df = covidcast.signal(data_source, signal, start_day=date_range[0], end_day=date_range[1],
                                           geo_type="state", time_type=time_type)
            if expected_df is None:
                raise RuntimeError("Data should exists")

            else:
                signal_df_dict[(data_source, signal, time_type)] = expected_df

    time.sleep(500)# TODO find a elegant way of seperating the logs for the calls from covidcast vs epidata
    print("-" * 90)
    for date_range, data_source_signal_list in signal_timeframe_dict.items():
        for data_source, signal in data_source_signal_list:
            expected_df = signal_df_dict.get((data_source, signal, date_range[2]))

            df = ported_signal(data_source, signal, start_day=date_range[0], end_day=date_range[1],
                               time_type=date_range[2],
                               geo_type="state")
            if not expected_df.empty:
                check = df.merge(expected_df, indicator=True)
                assert (check["_merge"] == "both").all()
            else:
                assert df.empty


if __name__ == "__main__":
    # check_metadata()
    check_signal()