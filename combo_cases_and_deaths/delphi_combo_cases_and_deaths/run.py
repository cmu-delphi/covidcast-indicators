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

from delphi_utils import read_params, add_prefix
from delphi_utils.geomap import GeoMapper
from .constants import METRICS, SMOOTH_TYPES, SENSORS, GEO_RESOLUTIONS


GMPR = GeoMapper()

def check_none_data_frame(data_frame, label, date_range):
    """Log and return True when a data frame is None."""
    if data_frame is None:
        print(f"{label} completely unavailable in range {date_range}")
        return True
    return False

def maybe_append(df1, df2):
    """
    Append dataframes if available, otherwise return non-None one.

    If both data frames are available, append them and return. Otherwise, return
    whichever frame is not None.
    """
    if df1 is None:
        return df2
    if df2 is None:
        return df1
    return df1.append(df2)

COLUMN_MAPPING = {"time_value": "timestamp",
                  "geo_value": "geo_id",
                  "value": "val",
                  "stderr": "se",
                  "sample_size": "sample_size"}
def combine_usafacts_and_jhu(signal, geo, date_range, fetcher=covidcast.signal):
    """Add rows for PR from JHU signals to USA-FACTS signals."""
    print("Fetching usa-facts...")
    # for hhs and nation, fetch the county data so we can combined JHU and USAFacts before mapping
    # to the desired geos.
    geo_to_fetch = "county" if geo in ["hhs", "nation"] else geo
    usafacts_df = fetcher("usa-facts", signal, date_range[0], date_range[1], geo_to_fetch)
    print("Fetching jhu-csse...")
    jhu_df = fetcher("jhu-csse", signal, date_range[0], date_range[1], geo_to_fetch)

    if check_none_data_frame(usafacts_df, "USA-FACTS", date_range) and \
       (geo_to_fetch not in ('state', 'county') or \
        check_none_data_frame(jhu_df, "JHU", date_range)):
        return pd.DataFrame({}, columns=COLUMN_MAPPING.values())

    # State level
    if geo_to_fetch == 'state':
        combined_df = maybe_append(
            usafacts_df,
            jhu_df if jhu_df is None else jhu_df[jhu_df["geo_value"] == 'pr']) # add territories
    # County level
    elif geo_to_fetch == 'county':
        combined_df = maybe_append(
            usafacts_df,
            jhu_df if jhu_df is None else jhu_df[jhu_df["geo_value"] == '72000'])
    # For MSA and HRR level, they are the same
    else:
        combined_df = usafacts_df
    combined_df.rename(COLUMN_MAPPING, axis=1, inplace=True)

    if geo in ["hhs", "nation"]:
        combined_df = GMPR.replace_geocode(combined_df,
                                           from_col="geo_id",
                                           from_code="fips",
                                           new_code=geo,
                                           date_col="timestamp")
        if "se" not in combined_df.columns and "sample_size" not in combined_df.columns:
            # if a column has non numeric data including None, they'll be dropped.
            # se and sample size are required later so we add them back.
            combined_df["se"] = combined_df["sample_size"] = None
        combined_df.rename({geo: "geo_id"}, axis=1, inplace=True)

    return combined_df

def extend_raw_date_range(params, sensor_name):
    """Extend the date range of the raw data backwards by 7 days.

    A complete issue includes smoothed signals as well as all raw data
    that contributed to the smoothed values, so that it's possible to use
    the raw values in the API to reconstruct the smoothed signal at will.
    The smoother we're currently using incorporates the previous 7
    days of data, so we must extend the date range of the raw data
    backwards by 7 days.
    """
    if sensor_name.find("7dav") < 0:
        return [
            params['date_range'][0] - timedelta(days=7),
            params['date_range'][-1]
            ]
    return params['date_range']

def next_missing_day(source, signals):
    """Fetch the first day for which we want to generate new data."""
    meta_df = covidcast.metadata()
    meta_df = meta_df[meta_df["data_source"] == source]
    meta_df = meta_df[meta_df["signal"].isin(signals)]
    # min: use the max_time of the most lagged signal, in case they differ
    # +timedelta: the subsequent day is the first day of new data to generate
    day = min(meta_df["max_time"]) + timedelta(days=1)
    return day

def sensor_signal(metric, sensor, smoother):
    """Generate the signal name for a particular configuration."""
    if smoother == "7dav":
        sensor_name = "_".join([smoother, sensor])
    else:
        sensor_name = sensor
    return sensor_name, "_".join([metric, sensor_name])

def configure(variants):
    """Validate params file and set date range."""
    params = read_params()
    params['export_start_date'] = date(*params['export_start_date'])
    yesterday = date.today() - timedelta(days=1)
    if params['date_range'] == 'new':
        # only create combined file for the newest update
        # (usually for yesterday, but check just in case)
        params['date_range'] = [
            min(
                yesterday,
                next_missing_day(
                    params["source"],
                    set(signal[-1] for signal in variants)
                )
            ),
            yesterday
        ]
    elif params['date_range'] == 'all':
        # create combined files for all of the historical reports
        params['date_range'] = [params['export_start_date'], yesterday]
    else:
        match_res = re.findall(re.compile(r'^\d{8}-\d{8}$'), params['date_range'])
        if len(match_res) == 0:
            raise ValueError(
                "Invalid date_range parameter. Please choose from (new, all, yyyymmdd-yyyymmdd).")
        try:
            date1 = datetime.strptime(params['date_range'][:8], '%Y%m%d').date()
        except ValueError as error:
            raise ValueError(
                "Invalid date_range parameter. Please check the first date.") from error
        try:
            date2 = datetime.strptime(params['date_range'][-8:], '%Y%m%d').date()
        except ValueError as error:
            raise ValueError(
                "Invalid date_range parameter. Please check the second date.") from error

        #The the valid start date
        if date1 < params['export_start_date']:
            date1 = params['export_start_date']
        params['date_range'] = [date1, date2]
    return params


def run_module():
    """Produce a combined cases and deaths signal using data from JHU and USA Facts."""
    variants = [tuple((metric, geo_res)+sensor_signal(metric, sensor, smoother))
                for (metric, geo_res, sensor, smoother) in
                product(METRICS, GEO_RESOLUTIONS, SENSORS, SMOOTH_TYPES)]
    params = configure(variants)
    for metric, geo_res, sensor_name, signal in variants:
        df = combine_usafacts_and_jhu(signal,
                                      geo_res,
                                      extend_raw_date_range(params, sensor_name))
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        start_date = pd.to_datetime(params['export_start_date'])
        export_dir = params["export_dir"]
        dates = pd.Series(
            df[df["timestamp"] >= start_date]["timestamp"].unique()
        ).sort_values()

        signal_name = add_prefix([signal], wip_signal=params["wip_signal"], prefix="wip_")
        for date_ in dates:
            export_fn = f'{date_.strftime("%Y%m%d")}_{geo_res}_{signal_name[0]}.csv'
            df[df["timestamp"] == date_][["geo_id", "val", "se", "sample_size", ]].to_csv(
                f"{export_dir}/{export_fn}", index=False, na_rep="NA"
            )
