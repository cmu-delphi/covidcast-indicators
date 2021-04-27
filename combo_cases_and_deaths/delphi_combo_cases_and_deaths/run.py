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

from delphi_utils import add_prefix, get_structured_logger, Nans
from delphi_utils.geomap import GeoMapper
from .constants import METRICS, SMOOTH_TYPES, SENSORS, GEO_RESOLUTIONS


GMPR = GeoMapper()

COLUMN_MAPPING = {"time_value": "timestamp",
                  "geo_value": "geo_id",
                  "value": "val",
                  "stderr": "se",
                  "sample_size": "sample_size"}

EMPTY_FRAME = pd.DataFrame({}, columns=COLUMN_MAPPING.values())

covidcast.covidcast._ASYNC_CALL = True  # pylint: disable=protected-access


def check_none_data_frame(data_frame, label, date_range):
    """Log and return True when a data frame is None."""
    if data_frame is None:
        print(f"{label} completely unavailable in range {date_range}")
        return True
    return False


def maybe_append(usa_facts, jhu):
    """
    Append dataframes if available, otherwise return USAFacts.

    If both data frames are available, append them and return.

    If only USAFacts is available, return it.

    If USAFacts is not available, return None.
    """
    if usa_facts is None:
        return None
    if jhu is None:
        return usa_facts
    return usa_facts.append(jhu)


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


def merge_dfs_by_geos(usafacts_df, jhu_df, geo):
    """Combine the queried usafacts and jhu dataframes based on the geo type."""
    # State level
    if geo == 'state':
        combined_df = maybe_append(
            usafacts_df,
            jhu_df if jhu_df is None else jhu_df[jhu_df["geo_value"] == 'pr']) # add territories
    # County level
    elif geo == 'county':
        combined_df = maybe_append(
            usafacts_df,
            jhu_df if jhu_df is None else jhu_df[jhu_df["geo_value"].str.startswith("72")])
    # For MSA and HRR level, they are the same
    elif geo == 'msa':
        df = GMPR._load_crosswalk("fips", "msa") # pylint: disable=protected-access
        puerto_rico_mask = df["fips"].str.startswith("72")
        puerto_rico_msas = df[puerto_rico_mask]["msa"].unique()
        combined_df = maybe_append(
            usafacts_df,
            jhu_df if jhu_df is None else jhu_df[jhu_df["geo_value"].isin(puerto_rico_msas)])
    else:
        combined_df = usafacts_df
    combined_df.rename(COLUMN_MAPPING, axis=1, inplace=True)

    return combined_df


def get_updated_dates(signal, geo, date_range, issue_range=None, fetcher=covidcast.signal):
    """Return the unique dates of the values that were updated in a given issue range in a geo."""
    usafacts_df = fetcher(
        "usa-facts", signal,
        date_range[0], date_range[1],
        geo,
        issues=issue_range
    )
    jhu_df = fetcher(
        "jhu-csse", signal,
        date_range[0], date_range[1],
        geo,
        issues=issue_range
    )

    if check_none_data_frame(usafacts_df, "USA-FACTS", date_range):
        return None

    merged_df = merge_dfs_by_geos(usafacts_df, jhu_df, geo)
    timestamp_mask = merged_df["timestamp"]<=usafacts_df["timestamp"].max()
    unique_dates = merged_df.loc[timestamp_mask]["timestamp"].unique()
    return unique_dates


def combine_usafacts_and_jhu(signal, geo, date_range, issue_range=None, fetcher=covidcast.signal):
    """Add rows for PR from JHU signals to USA-FACTS signals.

    For hhs and nation, fetch the county `num` data so we can compute the proportions correctly
    and after combining JHU and USAFacts and mapping to the desired geos.
    """
    is_special_geo = geo in ["hhs", "nation"]
    geo_to_fetch = "county" if is_special_geo else geo
    signal_to_fetch = signal.replace("_prop", "_num") if is_special_geo else signal

    unique_dates = get_updated_dates(
        signal_to_fetch, geo_to_fetch, date_range, issue_range, fetcher
    )

    # This occurs if the usafacts ~and the jhu query were empty
    if unique_dates is None:
        return EMPTY_FRAME

    # Query only the represented window so that every geo is represented; a single window call is
    # faster than a fetch for every date in unique_dates even in cases of 1:10 sparsity,
    # i.e., len(unique_dates):len(max(unique_dates) - min(unique_dates))
    query_min, query_max = unique_dates.min(), unique_dates.max()
    usafacts_df = fetcher(
        "usa-facts", signal_to_fetch,
        query_min, query_max,
        geo_to_fetch,
    )
    jhu_df = fetcher(
        "jhu-csse", signal_to_fetch,
        query_min, query_max,
        geo_to_fetch,
    )
    combined_df = merge_dfs_by_geos(usafacts_df, jhu_df, geo_to_fetch)

    # default sort from API is ORDER BY signal, time_value, geo_value, issue
    # we want to drop all but the most recent (last) issue
    combined_df.drop_duplicates(
        subset=["geo_id", "timestamp"],
        keep="last",
        inplace=True
    )

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
    next_day = next_missing_day(
        params['indicator']["source"],
        set(signal[-1] for signal in variants)
    )
    configure_range(params, 'date_range', yesterday, next_day)
    # pad issue range in case we caught jhu but not usafacts or v/v in the last N issues;
    # issue_days also needs to be set to a value large enough to include values you would like
    # to reissue
    try:
        issue_days = params['indicator']['issue_days']
    except KeyError:
        issue_days = 7
    configure_range(params, 'issue_range', yesterday, next_day - timedelta(days=issue_days))
    return params

def configure_range(params, range_param, yesterday, next_day):
    """Configure a parameter which stores a range of dates.

    May be specified in params.json as:
      "new" - set to [next_day, yesterday]
      "all" - set to [export_start_date, yesterday]
      yyyymmdd-yyyymmdd - set to exact range
    """
    if range_param not in params['indicator'] or params['indicator'][range_param] == 'new':
        # only create combined file for the newest update
        # (usually for yesterday, but check just in case)
        params['indicator'][range_param] = [
            min(
                yesterday,
                next_day
            ),
            yesterday
        ]
    elif params['indicator'][range_param] == 'all':
        # create combined files for all of the historical reports
        if range_param == 'date_range':
            params['indicator'][range_param] = [params['indicator']['export_start_date'], yesterday]
        elif range_param == 'issue_range':
            # for issue_range=all we want the latest issue for all requested
            # dates, aka the default when issue is unspecified
            params['indicator'][range_param] = None
        else:
            raise ValueError(
                f"Bad Programmer: Invalid range_param '{range_param}';"
                f"expected 'date_range' or 'issue_range'")
    else:
        match_res = re.findall(re.compile(r'^\d{8}-\d{8}$'), params['indicator'][range_param])
        if len(match_res) == 0:
            raise ValueError(
                f"Invalid {range_param} parameter. Try (new, all, yyyymmdd-yyyymmdd).")
        try:
            date1 = datetime.strptime(params['indicator'][range_param][:8], '%Y%m%d').date()
        except ValueError as error:
            raise ValueError(
                f"Invalid {range_param} parameter. Please check the first date.") from error
        try:
            date2 = datetime.strptime(params['indicator'][range_param][-8:], '%Y%m%d').date()
        except ValueError as error:
            raise ValueError(
                f"Invalid {range_param} parameter. Please check the second date.") from error

        # ensure valid start date
        if date1 < params['indicator']['export_start_date']:
            date1 = params['indicator']['export_start_date']
        params['indicator'][range_param] = [date1, date2]

def add_nancodes(df):
    """Add nancodes to the dataframe.

    se and sample_size should already be nan and NOT_APPLICABLE, inheriting from USAFacts
    and JHU. Due to the geo aggregation, the missingness codes will get mixed up among rows.
    So for the time being, we use only one missing code (UNKNOWN) for nan values in the val
    column.
    """
    # Default missingness codes
    df["missing_val"] = Nans.NOT_MISSING
    df["missing_se"] = Nans.NOT_APPLICABLE
    df["missing_sample_size"] = Nans.NOT_APPLICABLE

    # Missing codes for `val`
    missing_mask = df["val"].isnull()
    df.loc[missing_mask, "missing_val"] = Nans.UNKNOWN

    return df

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
                                      extend_raw_date_range(params, sensor_name),
                                      params['indicator']['issue_range'])
        df = add_nancodes(df)
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
            date_mask = (df["timestamp"] == date_)
            columns_to_write = [
                "geo_id", "val", "se", "sample_size",
                "missing_val", "missing_se", "missing_sample_size"
            ]
            df.loc[date_mask, columns_to_write].to_csv(
                f"{export_dir}/{export_fn}", index=False, na_rep="NA"
            )

    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    logger.info("Completed indicator run",
        elapsed_time_in_seconds = elapsed_time_in_seconds)
