# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed when
the module is run with `python -m delphi_combo_cases_and_deaths`.

This module produces a combined signal for jhu-csse and usa-facts.  This signal
is only used for visualization.  It sources Puerto Rico from jhu-csse and
everything else from usa-facts.

"""
from datetime import date, timedelta, datetime
from itertools import product
import re

import covidcast
import pandas as pd

from delphi_utils import read_params, create_export_csv


METRICS = [
    "confirmed",
    "deaths",
]
SMOOTH_TYPES = [
    "",
    "7dav",
]
SENSORS = [
    "incidence_num",
    "cumulative_num",
    "incidence_prop",
    "cumulative_prop",
]
GEO_RESOLUTIONS = [
    "county",
    "state",
    "msa",
    "hrr",
]

def combine_usafacts_and_jhu(signal, geo, date_range):
    """
    Add rows for PR from JHU signals to USA-FACTS signals
    """
    usafacts_df = covidcast.signal("usa-facts", signal, date_range[0], date_range[1], geo)
    jhu_df = covidcast.signal("jhu-csse", signal, date_range[0], date_range[1], geo)
    # State level
    if geo == 'state':
        combined_df = usafacts_df.append(jhu_df[jhu_df["geo_value"] == 'pr'])
    # County level
    elif geo == 'county':
        combined_df = usafacts_df.append(jhu_df[jhu_df["geo_value"] == '72000'])
    # For MSA and HRR level, they are the same
    else:
        combined_df = usafacts_df

    combined_df = combined_df.drop(["direction"], axis=1)
    combined_df = combined_df.rename({"time_value": "timestamp",
                                      "geo_value": "geo_id",
                                      "value": "val",
                                      "stderr": "se"},
                                     axis=1)
    return combined_df

def next_missing_day(source, signals):
    """Fetch the first day for which we want to generate new data."""
    meta_df = covidcast.metadata()
    meta_df = meta_df[meta_df["data_source"]==source]
    meta_df = meta_df[meta_df["signal"].isin(signals)]
    # min: use the max_time of the most lagged signal, in case they differ
    # +timedelta: the subsequent day is the first day of new data to generate
    day = min(meta_df["max_time"])+timedelta(days=1)
    return day

def run_module():
    """Produce a combined cases and deaths signal using data from JHU and USA Facts"""
    def sensor_signal(metric, sensor, smoother):
        if smoother == "7dav":
            sensor_name = "_".join([smoother, sensor])
        else:
            sensor_name = sensor
        signal = "_".join([metric, sensor_name])
        return sensor_name, signal
    variants = [tuple((metric, geo_res)+sensor_signal(metric, sensor, smoother))
                for (metric, geo_res, sensor, smoother) in
                product(METRICS, GEO_RESOLUTIONS, SENSORS, SMOOTH_TYPES)]

    params = read_params()
    params['export_start_date'] = date(*params['export_start_date'])
    yesterday = date.today() - timedelta(days=1)
    if params['date_range'] == 'new':
        # only create combined file for the newest update
        # (usually for yesterday, but check just in case)
        params['date_range'] = [
            min(yesterday, next_missing_day(params["source"], set(signal[-1] for signal in variants))),
            yesterday
        ]
    elif params['date_range'] == 'all':
        # create combined files for all of the historical reports
        params['date_range'] = [params['export_start_date'], yesterday]
    else:
        pattern = re.compile(r'^\d{8}-\d{8}$')
        match_res = re.findall(pattern, params['date_range'])
        if len(match_res) == 0:
            raise ValueError(
                "Invalid date_range parameter. Please choose from (new, all, yyyymmdd-yyyymmdd).")
        try:
            date1 = datetime.strptime(params['date_range'][:8], '%Y%m%d').date()
        except ValueError:
            raise ValueError("Invalid date_range parameter. Please check the first date.")
        try:
            date2 = datetime.strptime(params['date_range'][-8:], '%Y%m%d').date()
        except ValueError:
            raise ValueError("Invalid date_range parameter. Please check the second date.")

        #The the valid start date
        if date1 < params['export_start_date']:
            date1 = params['export_start_date']
        params['date_range'] = [date1, date2]

    for metric, geo_res, sensor_name, signal in variants:
        create_export_csv(
            combine_usafacts_and_jhu(signal, geo_res, params['date_range']),
            export_dir=params['export_dir'],
            start_date=pd.to_datetime(params['export_start_date']),
            metric=metric,
            geo_res=geo_res,
            sensor=sensor_name,
        )
