from datetime import datetime, date, timedelta
from typing import List, Tuple, Union, Iterable

import pandas as pd

from delphi_epidata import Epidata

def date_generator(startdate, enddate):
  while startdate <= enddate:
    yield startdate.strftime('%Y-%m-%d')
    startdate = startdate + timedelta(days=1)



def metadata():
    response = Epidata._request("covidcast_meta")

    if response["result"] != 1:
        # Something failed in the API and we did not get real metadata
        raise RuntimeError("Error when fetching metadata from the API",
                           response["message"])

    df = pd.DataFrame.from_dict(response["epidata"])
    return df


def signal(
    data_source: str,
    signal: str,  # pylint: disable=W0621
    start_day: date = None,
    end_day: date = None,
    geo_type: str = "county",
    geo_values: Union[str, Iterable[str]] = "*",
    as_of: date = None,
    issues: Union[date, Tuple[date], List[date]] = None,
    lag: int = None,
    time_type: str = "day",
) -> Union[pd.DataFrame, None]:
    """Download a Pandas data frame for one signal.

    Obtains data for selected date ranges for all geographic regions of the
    United States. Available data sources and signals are documented in the
    `COVIDcast signal documentation
    <https://cmu-delphi.github.io/delphi-epidata/api/covidcast_signals.html>`_.
    Most (but not all) data sources are available at the county level, but the
    API can also return data aggregated to metropolitan statistical areas,
    hospital referral regions, or states, as desired, by using the ``geo_type``
    argument.

    The COVIDcast API tracks updates and changes to its underlying data, and
    records the first date each observation became available. For example, a
    data source may report its estimate for a specific state on June 3rd on June
    5th, once records become available. This data is considered "issued" on June
    5th. Later, the data source may update its estimate for June 3rd based on
    revised data, creating a new issue on June 8th. By default, ``signal()``
    returns the most recent issue available for every observation. The
    ``as_of``, ``issues``, and ``lag`` parameters allow the user to select
    specific issues instead, or to see all updates to observations. These
    options are mutually exclusive; if you specify more than one, ``as_of`` will
    take priority over ``issues``, which will take priority over ``lag``.

    Note that the API only tracks the initial value of an estimate and *changes*
    to that value. If a value was first issued on June 5th and never updated,
    asking for data issued on June 6th (using ``issues`` or ``lag``) would *not*
    return that value, though asking for data ``as_of`` June 6th would.

    Note also that the API enforces a maximum result row limit; results beyond
    the maximum limit are truncated. This limit is sufficient to fetch
    observations in all counties in the United States on one day. This client
    automatically splits queries for multiple days across multiple API calls.
    However, if data for one day has been issued many times, using the
    ``issues`` argument may return more results than the query limit. A warning
    will be issued in this case. To see all results, split your query across
    multiple calls with different ``issues`` arguments.

    See the `COVIDcast API documentation
    <https://cmu-delphi.github.io/delphi-epidata/api/covidcast.html>`_ for more
    information on available geography types, signals, and data formats, and
    further discussion of issue dates and data versioning.

    :param data_source: String identifying the data source to query, such as
      ``"fb-survey"``.
    :param signal: String identifying the signal from that source to query,
      such as ``"smoothed_cli"``.
    :param start_day: Query data beginning on this date. Provided as a
      ``datetime.date`` object. If ``start_day`` is ``None``, defaults to the
      first day data is available for this signal. If ``time_type == "week"``, then
      this is rounded to the epiweek containing the day (i.e. the previous Sunday).
    :param end_day: Query data up to this date, inclusive. Provided as a
      ``datetime.date`` object. If ``end_day`` is ``None``, defaults to the most
      recent day data is available for this signal. If ``time_type == "week"``, then
      this is rounded to the epiweek containing the day (i.e. the previous Sunday).
    :param geo_type: The geography type for which to request this data, such as
      ``"county"`` or ``"state"``. Available types are described in the
      COVIDcast signal documentation. Defaults to ``"county"``.
    :param geo_values: The geographies to fetch data for. The default, ``"*"``,
      fetches all geographies. To fetch one geography, specify its ID as a
      string; multiple geographies can be provided as an iterable (list, tuple,
      ...) of strings.
    :param as_of: Fetch only data that was available on or before this date,
      provided as a ``datetime.date`` object. If ``None``, the default, return
      the most recent available data. If ``time_type == "week"``, then
      this is rounded to the epiweek containing the day (i.e. the previous Sunday).
    :param issues: Fetch only data that was published or updated ("issued") on
      these dates. Provided as either a single ``datetime.date`` object,
      indicating a single date to fetch data issued on, or a tuple or list
      specifying (start, end) dates. In this case, return all data issued in
      this range. There may be multiple rows for each observation, indicating
      several updates to its value. If ``None``, the default, return the most
      recently issued data. If ``time_type == "week"``, then these are rounded to
      the epiweek containing the day (i.e. the previous Sunday).
    :param lag: Integer. If, for example, ``lag=3``, fetch only data that was
      published or updated exactly 3 days after the date. For example, a row
      with ``time_value`` of June 3 will only be included in the results if its
      data was issued or updated on June 6. If ``None``, the default, return the
      most recently issued data regardless of its lag.
    :param time_type: The temporal resolution to request this data. Most signals
      are available at the "day" resolution (the default); some are only
      available at the "week" resolution, representing an MMWR week ("epiweek").
    :returns: A Pandas data frame with matching data, or ``None`` if no data is
      returned. Each row is one observation on one day in one geographic location.
      Contains the following columns:

      ``geo_value``
        Identifies the location, such as a state name or county FIPS code. The
        geographic coding used by COVIDcast is described in the `API
        documentation here
        <https://cmu-delphi.github.io/delphi-epidata/api/covidcast_geography.html>`_.

      ``signal``
        Name of the signal, same as the value of the ``signal`` input argument. Used for
        downstream functions to recognize where this signal is from.

      ``time_value``
        Contains a `pandas Timestamp object
        <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Timestamp.html>`_
        identifying the date this estimate is for. For data with ``time_type = "week"``, this
        is the first day of the corresponding epiweek.

      ``issue``
        Contains a `pandas Timestamp object
        <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Timestamp.html>`_
        identifying the date this estimate was issued. For example, an estimate
        with a ``time_value`` of June 3 might have been issued on June 5, after
        the data for June 3rd was collected and ingested into the API.

      ``lag``
        Integer giving the difference between ``issue`` and ``time_value``,
        in days.

      ``value``
        The signal quantity requested. For example, in a query for the
        ``confirmed_cumulative_num`` signal from the ``usa-facts`` source,
        this would be the cumulative number of confirmed cases in the area, as
        of the ``time_value``.

      ``stderr``
        The value's standard error, if available.

      ``sample_size``
        Indicates the sample size available in that geography on that day;
        sample size may not be available for all signals, due to privacy or
        other constraints.

      ``geo_type``
        Geography type for the signal, same as the value of the ``geo_type`` input argument.
        Used for downstream functions to parse ``geo_value`` correctly

      ``data_source``
        Name of the signal source, same as the value of the ``data_source`` input argument. Used for
        downstream functions to recognize where this signal is from.

    Consult the `signal documentation
    <https://cmu-delphi.github.io/delphi-epidata/api/covidcast_signals.html>`_
    for more details on how values and standard errors are calculated for
    specific signals.

    """
    if start_day > end_day:
        raise ValueError(
            "end_day must be on or after start_day, but " f"start_day = '{start_day}', end_day = '{end_day}'"
        )

    time_values = list(date_generator(start_day, end_day))
    issues = list(date_generator(start_day, end_day)) #TODO placesholder
    response = Epidata.covidcast(data_source, signal, time_type=time_type,
                                 geo_type=geo_type, time_values=time_values,
                                 geo_value=geo_values, as_of=as_of,
                                 issues=issues, lag=lag)
    if response["result"] != 1:
        # Something failed in the API and we did not get real metadata
        raise RuntimeError("Error when fetching metadata from the API",
                           response["message"])

    api_df = pd.DataFrame.from_dict(response["epidata"])
    api_df["issue"] = pd.to_datetime(api_df["issue"], format='%Y%m%d')
    api_df["time_value"] = pd.to_datetime(api_df["time_value"], format='%Y%m%d')
    api_df.drop("direction", axis=1, inplace=True)
    api_df["data_source"] = data_source
    api_df["signal"] = signal

    return api_df
