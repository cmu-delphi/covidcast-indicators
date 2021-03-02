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
import time

import covidcast
import pandas as pd

from delphi_utils import add_prefix, get_structured_logger
from delphi_utils.geomap import GeoMapper
from .constants import METRICS, SMOOTH_TYPES, SENSORS, GEO_RESOLUTIONS


GMPR = GeoMapper()

COLUMN_MAPPING = {"time_value": "timestamp",
                  "geo_value": "geo_id",
                  "value": "val",
                  "stderr": "se",
                  "sample_size": "sample_size"}

covidcast.covidcast._ASYNC_CALL = True  # pylint: disable=protected-access


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


def compute_special_geo_dfs(df, signal, geo):
    """Compute the signal values for special geos (HHS and nation).

    For `num` signals, just replace the geocode to the appropriate resolution.
    For `prop` signals, replace the geocode and then compute the proportion using the total
    population of the us.

    Parameters
    ----------
    df: DataFrame
        Dataframe with num values at the county level.
    signal: str
        Signal name, should end with 'num' or 'prop'.
    geo: str
        Geo level to compute.
    Returns
    -------
        DataFrame mapped to the 'geo' level with the correct signal values computed.
    """
    df = GMPR.replace_geocode(df,
                              from_col="geo_id",
                              from_code="fips",
                              new_code="state_code",
                              date_col="timestamp")
    df = GMPR.add_population_column(df, "state_code")  # use total state population
    df = GMPR.replace_geocode(df, from_code="state_code", new_code=geo, date_col="timestamp")
    if signal.endswith("_prop"):
        df["val"] = df["val"]/df["population"] * 100000
    df.drop("population", axis=1, inplace=True)
    df.rename({geo: "geo_id"}, axis=1, inplace=True)
    return df


def combine_usafacts_and_jhu(signal, geo, date_range, fetcher=covidcast.signal):
    """Add rows for PR from JHU signals to USA-FACTS signals.

    For hhs and nation, fetch the county `num` data so we can compute the proportions correctly
    and after combining JHU and USAFacts and mapping to the desired geos.
    """
    is_special_geo = geo in ["hhs", "nation"]
    geo_to_fetch = "county" if is_special_geo else geo
    signal_to_fetch = signal.replace("_prop", "_num") if is_special_geo else signal
    print("Fetching usa-facts...")
    usafacts_df = fetcher("usa-facts", signal_to_fetch, date_range[0], date_range[1], geo_to_fetch)
    print("Fetching jhu-csse...")
    jhu_df = fetcher("jhu-csse", signal_to_fetch, date_range[0], date_range[1], geo_to_fetch)
    if check_none_data_frame(usafacts_df, "USA-FACTS", date_range) and \
       (geo_to_fetch not in ('state', 'county') or
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
            jhu_df if jhu_df is None else jhu_df[jhu_df["geo_value"].str.startswith("72")])
    # For MSA and HRR level, they are the same
    else:
        combined_df = usafacts_df
    combined_df.rename(COLUMN_MAPPING, axis=1, inplace=True)

    if is_special_geo:
        combined_df = compute_special_geo_dfs(combined_df, signal, geo)
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
            params['indicator']['date_range'][0] - timedelta(days=7),
            params['indicator']['date_range'][-1]
            ]
    return params['indicator']['date_range']

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

def configure(variants, params):
    """Validate params file and set date range."""
    params['indicator']['export_start_date'] = date(*params['indicator']['export_start_date'])
    yesterday = date.today() - timedelta(days=1)
    if params['indicator']['date_range'] == 'new':
        # only create combined file for the newest update
        # (usually for yesterday, but check just in case)
        params['indicator']['date_range'] = [
            min(
                yesterday,
                next_missing_day(
                    params['indicator']["source"],
                    set(signal[-1] for signal in variants)
                )
            ),
            yesterday
        ]
    elif params['indicator']['date_range'] == 'all':
        # create combined files for all of the historical reports
        params['indicator']['date_range'] = [params['indicator']['export_start_date'], yesterday]
    else:
        match_res = re.findall(re.compile(r'^\d{8}-\d{8}$'), params['indicator']['date_range'])
        if len(match_res) == 0:
            raise ValueError(
                "Invalid date_range parameter. Please choose from (new, all, yyyymmdd-yyyymmdd).")
        try:
            date1 = datetime.strptime(params['indicator']['date_range'][:8], '%Y%m%d').date()
        except ValueError as error:
            raise ValueError(
                "Invalid date_range parameter. Please check the first date.") from error
        try:
            date2 = datetime.strptime(params['indicator']['date_range'][-8:], '%Y%m%d').date()
        except ValueError as error:
            raise ValueError(
                "Invalid date_range parameter. Please check the second date.") from error

        #The the valid start date
        if date1 < params['indicator']['export_start_date']:
            date1 = params['indicator']['export_start_date']
        params['indicator']['date_range'] = [date1, date2]
    return params


def run_module(params):
    """
    Produce a combined cases and deaths signal using data from JHU and USA Facts.

    Parameters
    ----------
    params
        Dictionary containing indicator configuration. Expected to have the following structure:
        - "common":
            - "export_dir": str, directory to write output.
            - "log_exceptions" (optional): bool, whether to log exceptions to file.
            - "log_filename" (optional): str, name of file to write logs
        - "indicator":
            - "export_start_date": list of ints, [year, month, day] format, first day to begin
                data exports from.
            - "date_range": str, YYYYMMDD-YYYYMMDD format, range of dates to generate data for.
            - "source": str, name of combo indicator in metadata.
            - "wip_signal": list of str or bool, to be passed to delphi_utils.add_prefix.
    """
    start_time = time.time()
    variants = [tuple((metric, geo_res)+sensor_signal(metric, sensor, smoother))
                for (metric, geo_res, sensor, smoother) in
                product(METRICS, GEO_RESOLUTIONS, SENSORS, SMOOTH_TYPES)]
    params = configure(variants, params)
    logger = get_structured_logger(
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))

    for metric, geo_res, sensor_name, signal in variants:
        df = combine_usafacts_and_jhu(signal,
                                      geo_res,
                                      extend_raw_date_range(params, sensor_name))
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        start_date = pd.to_datetime(params['indicator']['export_start_date'])
        export_dir = params["common"]["export_dir"]
        dates = pd.Series(
            df[df["timestamp"] >= start_date]["timestamp"].unique()
        ).sort_values()

        signal_name = add_prefix([signal],
                                 wip_signal=params['indicator']["wip_signal"],
                                 prefix="wip_")
        for date_ in dates:
            export_fn = f'{date_.strftime("%Y%m%d")}_{geo_res}_{signal_name[0]}.csv'
            df[df["timestamp"] == date_][["geo_id", "val", "se", "sample_size", ]].to_csv(
                f"{export_dir}/{export_fn}", index=False, na_rep="NA"
            )

    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    logger.info("Completed indicator run",
        elapsed_time_in_seconds = elapsed_time_in_seconds)
