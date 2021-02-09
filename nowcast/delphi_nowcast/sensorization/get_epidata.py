import asyncio
from datetime import datetime, date
from typing import Tuple, List, Dict
from itertools import product

from numpy import isnan
from pandas import date_range
from aiohttp import ClientSession

# from ..delphi_epidata import Epidata  # used for local testing
from delphi_epidata import Epidata

from ..data_containers import LocationSeries, SensorConfig

EPIDATA_START_DATE = 20200101

async def get(params, session, sensor, location):
    """Helper function to make Epidata GET requests."""
    async with session.get(Epidata.BASE_URL, params=params) as response:
        return await response.json(), sensor, location


async def fetch_epidata(combos, as_of):
    """Helper function to asynchronously make and aggregate Epidata GET requests."""
    tasks = []
    async with ClientSession() as session:
        for sensor, location in combos:
            params = {
                    "endpoint": "covidcast",
                    "data_source": sensor.source,
                    "signals": sensor.signal,
                    "time_type": "day",
                    "geo_type": location.geo_type,
                    "time_values": f"{EPIDATA_START_DATE}-{as_of}",
                    "geo_value": location.geo_value,
                    "as_of": as_of
                }
            task = asyncio.create_task(get(params, session, sensor, location))
            tasks.append(task)
        responses = await asyncio.gather(*tasks)
        return responses


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
    """
    # gets all available data up to as_of day for now, could be optimized to only get a window
    output = {}
    all_combos = product(sensors, locations)
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(fetch_epidata(all_combos, as_of.strftime("%Y%m%d")))
    responses = loop.run_until_complete(future)
    for response, sensor, location in responses:
        # -2 = no results, 1 = success. Truncated data or server errors may lead to this Exception.
        if response["result"] not in (-2, 1):
            raise Exception(f"Bad result from Epidata: {response['message']}")
        data = LocationSeries(
            geo_value=location.geo_value,
            geo_type=location.geo_type,
            data={datetime.strptime(i["time_value"], "%Y%m%d").date(): i["value"]
                  for i in response.get("epidata", []) if not isnan(i["value"])}
        )
        if data.data:
            output[(sensor.source, sensor.signal, location.geo_type, location.geo_value)] = data
    return output


def get_historical_sensor_data(sensor: SensorConfig,
                               geo_value: str,
                               geo_type: str,
                               end_date: date,
                               start_date: date) -> Tuple[LocationSeries, list]:
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
    if response["result"] == 1:
        output = LocationSeries(
            geo_value=geo_value,
            geo_type=geo_type,
            data={datetime.strptime(str(i["time_value"]), "%Y%m%d").date(): i["value"]
                  for i in response.get("epidata", []) if not isnan(i["value"])}
        )
    elif response["result"] == -2:  # no results
        print("No historical results found")
        output = LocationSeries(geo_value=geo_value, geo_type=geo_type)
    else:
        raise Exception(f"Bad result from Epidata: {response['message']}")
    all_dates = [i.date() for i in date_range(start_date, end_date)]
    missing_dates = [i for i in all_dates if i not in output.dates]
    return output, missing_dates
