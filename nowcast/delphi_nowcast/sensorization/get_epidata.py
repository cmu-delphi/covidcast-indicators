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
                    "time_values": f"20200101-{as_of}",
                    "geo_value": location.geo_value,
                    "as_of": as_of
                }
            task = asyncio.ensure_future(get(params, session, sensor, location))
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
        if response["result"] not in (-2, 1):
            raise Exception(f"Bad result from Epidata: {response['message']}")
        data = LocationSeries(
            geo_value=location.geo_value,
            geo_type=location.geo_type,
            dates=[datetime.strptime(i, "%Y%m%d").date() for i in response.get("epidata", [])
                   if not isnan(i["value"])],
            values=[i["value"] for i in response.get("epidata", []) if not isnan(i["value"])]
        )
        if not data.empty:
            output[(sensor.source, sensor.signal, location.geo_type, location.geo_value)] = data
    return output
