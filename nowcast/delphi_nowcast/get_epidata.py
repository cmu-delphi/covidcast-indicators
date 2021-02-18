"""Retrieve data from Epidata API."""
from datetime import datetime, date
from itertools import product
from typing import Tuple, List, Dict

from delphi_epidata import Epidata
from numpy import isnan
from pandas import date_range

from .data_containers import LocationSeries, SensorConfig

EPIDATA_START_DATE = 20200101


def get_indicator_data(sensors: List[SensorConfig],
                       locations: List[LocationSeries],
                       as_of: date) -> Dict[Tuple, LocationSeries]:
    """
    Given a list of sensors and locations, asynchronously gets covidcast data for all combinations.

    Parameters
    ----------
    sensors
        list of SensorConfigs for sensors to retrieve.
    locations
        list of LocationSeries, one for each location desired. This is only used for the list of
        locations; none of the dates or values are used.
    as_of
        Date that the data should be retrieved as of.
    Returns
    -------
        Dictionary of {(source, signal, geo_type, geo_value): LocationSeries} containing indicator
        data,
    """
    # gets all available data up to as_of day for now, could be optimized to only get a window
    output = {}
    all_combos = product(sensors, locations)
    as_of_str = as_of.strftime("%Y%m%d")
    all_params = [
        {"source": "covidcast",
         "data_source": sensor.source,
         "signals": sensor.signal,
         "time_type": "day",
         "geo_type": location.geo_type,
         "geo_value": location.geo_value,
         "time_values": f"{EPIDATA_START_DATE}-{as_of_str}",
         "as_of": as_of_str}
        for sensor, location in all_combos
    ]
    responses = Epidata.async_epidata(all_params)
    for response, params in responses:
        # -2 = no results, 1 = success. Truncated data or server errors may lead to this Exception.
        if response["result"] not in (-2, 1):
            raise Exception(f"Bad result from Epidata: {response['message']}")
        data = LocationSeries(
            geo_value=params["geo_value"],
            geo_type=params["geo_type"],
            data={datetime.strptime(str(i["time_value"]), "%Y%m%d").date(): i["value"]
                  for i in response.get("epidata", []) if not isnan(i["value"])}
        )
        if data.data:
            output[(params["data_source"],
                    params["signals"],
                    params["geo_type"],
                    params["geo_value"])] = data
    return output


def get_historical_sensor_data(sensor: SensorConfig,
                               geo_value: str,
                               geo_type: str,
                               start_date: date,
                               end_date: date) -> Tuple[LocationSeries, list]:
    """
    Query Epidata API for historical sensorization data.

    Will only return values if they are not null. If any days are null or are not available,
    they will be listed as missing.

    Parameters
    ----------
    sensor
        SensorConfig specifying which sensor to retrieve.
    geo_type
        Geo type to retrieve.
    geo_value
        Geo value to retrieve.
    start_date
        First day to retrieve (inclusive).
    end_date
        Last day to retrieve (inclusive).
    Returns
    -------
        Tuple of (LocationSeries containing non-na data, list of dates without valid data). If no
        data was found, an empty LocationSeries is returned.
    """
    response = Epidata.covidcast_nowcast(
        data_source=sensor.source,
        signals=sensor.signal,
        time_type="day",
        geo_type=geo_type,
        time_values=Epidata.range(start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")),
        geo_value=geo_value,
        sensor_names=sensor.name,
        lag=sensor.lag)
    all_dates = [i.date() for i in date_range(start_date, end_date)]
    if response["result"] == 1:
        output = LocationSeries(
            geo_value=geo_value,
            geo_type=geo_type,
            data={datetime.strptime(str(i["time_value"]), "%Y%m%d").date(): i["value"]
                  for i in response.get("epidata", []) if not isnan(i["value"])}
        )
        missing_dates = [i for i in all_dates if i not in output.dates]
        return output, missing_dates
    if response["result"] == -2:  # no results
        print("No historical results found")
        output = LocationSeries(geo_value=geo_value, geo_type=geo_type)
        return output, all_dates
    raise Exception(f"Bad result from Epidata: {response['message']}")
