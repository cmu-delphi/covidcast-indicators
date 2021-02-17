"""Functions to run sensorization."""
import os
from collections import defaultdict
from typing import List, DefaultDict
from datetime import timedelta, date

import numpy as np

from .ar_model import compute_ar_sensor
from .regression_model import compute_regression_sensor
from ..get_epidata import get_indicator_data, get_historical_sensor_data
from ..data_containers import LocationSeries, SensorConfig
from ..constants import AR_ORDER, AR_LAMBDA, REG_INTERCEPT


def compute_sensors(as_of_date: date,
                    regression_sensors: List[SensorConfig],
                    ground_truth_sensor: SensorConfig,
                    ground_truths: List[LocationSeries],
                    export_dir: str = "",
                    ) -> DefaultDict[SensorConfig, List[LocationSeries]]:
    """
    Parameters
    ----------
    as_of_date
        Date that the data should be retrieved as of.
    regression_sensors
        list of SensorConfigs for regression sensors to compute.
    ground_truth_sensor
        SensorConfig of the ground truth signal which is used for the AR sensor.
    ground_truths
        list of LocationSeries, one for each location desired.
    export_dir
        string of directory to output data. If empty string, no output will be exported.
    Returns
    -------
        Dict where keys are sensor tuples and values are lists, where each list element is a
        LocationSeries holding sensor data for a location. Each LocationSeries will only have a
        single value for the date (as_of_date - lag), e.g. if as_of_date is 20210110 and lag=5,
        the output will be values for 20200105.
    """
    output = defaultdict(list)
    indicator_data = get_indicator_data(regression_sensors, ground_truths, as_of_date)
    for loc in ground_truths:
        ground_truth_pred_date = as_of_date - timedelta(ground_truth_sensor.lag)
        ar_sensor = compute_ar_sensor(ground_truth_pred_date, loc, AR_ORDER, AR_LAMBDA)
        if not np.isnan(ar_sensor):
            output[ground_truth_sensor].append(
                LocationSeries(loc.geo_value, loc.geo_type, {ground_truth_pred_date: ar_sensor})
            )
        for sensor in regression_sensors:
            sensor_pred_date = as_of_date - timedelta(sensor.lag)
            covariates = indicator_data.get(
                (sensor.source, sensor.signal, loc.geo_type, loc.geo_value)
            )
            if not covariates:
                # TODO convert to log statements #689
                print(f"No data: {(sensor.source, sensor.signal, loc.geo_type, loc.geo_value)}")
                continue
            reg_sensor = compute_regression_sensor(sensor_pred_date, covariates, loc, REG_INTERCEPT)
            if not np.isnan(reg_sensor):
                output[sensor].append(
                    LocationSeries(loc.geo_value, loc.geo_type, {sensor_pred_date: reg_sensor})
                )
    if export_dir:
        for sensor, locations in output.items():
            for loc in locations:
                print(_export_to_csv(loc, sensor, as_of_date, export_dir))
    return output


def historical_sensors(start_date: date,
                       end_date: date,
                       sensors: List[SensorConfig],
                       ground_truths: List[LocationSeries],
                       ) -> DefaultDict[SensorConfig, List[LocationSeries]]:
    """
    Retrieve past sensorized values from start to end date at given locations for specified sensors.
    Parameters
    ----------
    start_date
        first day to attempt to get sensor values for.
    end_date
        last day to attempt to get sensor values for.
    sensors
        list of SensorConfigs for sensors to retrieve.
    ground_truths
        list of LocationSeries, one for each location desired. This is only used for the list of
        locations; none of the dates or values are used.
    Returns
    -------
        Dict where keys are sensor tuples and values are lists, where each list element is a
        LocationSeries holding sensor data for a location.
    """
    output = defaultdict(list)
    for location in ground_truths:
        for sensor in sensors:
            sensor_vals, missing_dates = get_historical_sensor_data(
                sensor, location.geo_value, location.geo_type, start_date, end_date
            )
            if sensor_vals.data:
                output[sensor].append(sensor_vals)
    return output


def _export_to_csv(value: LocationSeries,
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
