import asyncio
from datetime import datetime, date
import os
from typing import Tuple, List, Dict
from itertools import product

from numpy import isnan
from pandas import date_range
from aiohttp import ClientSession, ContentTypeError
from json import JSONDecodeError
# from ..delphi_epidata import Epidata  # used for local testing
from delphi_epidata import Epidata
from tenacity import AsyncRetrying, RetryError, stop_after_attempt

from .data_containers import LocationSeries, SensorConfig

EPIDATA_START_DATE = 20200101

async def get(params, session, sensor, location):
    """Helper function to make Epidata GET requests."""
    try:
        async for attempt in AsyncRetrying(stop=stop_after_attempt(4)):
            with attempt:
                async with session.get(Epidata.BASE_URL, params=params) as response:
                    return await response.json(), sensor, location
    except RetryError:
        pass

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
                    "as_of": as_of,
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
    responses = [i for i in responses if i]
    for response, sensor, location in responses:
        # -2 = no results, 1 = success. Truncated data or server errors may lead to this Exception.
        if response["result"] not in (-2, 1):
            raise Exception(f"Bad result from Epidata: {response['message']}")
        data = LocationSeries(
            geo_value=location.geo_value,
            geo_type=location.geo_type,
            data={datetime.strptime(str(i["time_value"]), "%Y%m%d").date(): i["value"]
                  for i in response.get("epidata", []) if not isnan(i["value"])}
        )
        if data.data:
            output[(sensor.source, sensor.signal, location.geo_type, location.geo_value)] = data
    return output


def get_historical_sensor_data(sensor: SensorConfig,
                               location: LocationSeries,
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
    location
        LocationSeries for the location to get.
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
        geo_type=location.geo_type,
        time_values=Epidata.range(start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")),
        geo_value=location.geo_value,
        sensor_names=sensor.name,
        lag=sensor.lag)
    if response["result"] == 1:
        location.data = {datetime.strptime(str(i["time_value"]), "%Y%m%d").date(): i["value"]
                         for i in response.get("epidata", []) if not isnan(i["value"])}
    elif response["result"] == -2:  # no results
        print("No historical results found")
    else:
        raise Exception(f"Bad result from Epidata: {response['message']}")
    all_dates = [i.date() for i in date_range(start_date, end_date)]
    missing_dates = [i for i in all_dates if i not in location.dates]
    return location, missing_dates


def export_to_csv(value: LocationSeries,
                   sensor: SensorConfig,
                   as_of_date: date,
                   receiving_dir: str
                   ) -> List[str]:
    """
    Save value to csv for upload to Epidata database.
    Parameters
    ----------
    value
        LocationSeries containing data.
    sensor
        SensorConfig corresponding to value.
    as_of_date
        As_of date for the indicator data used to train the sensor.
    receiving_dir
        Export directory for Epidata acquisition.
    Returns
    -------
        Filepath of exported files
    """
    export_dir = os.path.join(
        receiving_dir,
        f"issue_{as_of_date.strftime('%Y%m%d')}",
        sensor.source
    )
    os.makedirs(export_dir, exist_ok=True)
    exported_files = []
    for time_value in value.dates:
        export_file = os.path.join(
            export_dir,
            f"{time_value.strftime('%Y%m%d')}_{value.geo_type}_{sensor.signal}.csv"
        )
        if os.path.exists(export_file):
            with open(export_file, "a") as f:
                f.write(
                    f"{sensor.name},{value.geo_value},{value.data.get(time_value, '')}\n")
        else:
            with open(export_file, "a") as f:
                f.write("sensor_name,geo_value,value\n")
                f.write(
                    f"{sensor.name},{value.geo_value},{value.data.get(time_value, '')}\n")
        exported_files.append(export_file)
    return exported_files
