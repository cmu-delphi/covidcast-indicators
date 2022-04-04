# -*- coding: utf-8 -*-
"""Process and export Safegraph patterns signal."""
import glob
import os
from itertools import product

from datetime import date
import numpy as np
import pandas as pd
from delphi_utils import create_export_csv, GeoMapper
from .pull import pull

INCIDENCE_BASE = 100000

GEO_KEY_DICT = {
        "county": "fips",
        "msa": "msa",
        "hrr": "hrr",
        "state": "state_id",
        "hhs": "hhs",
        "nation": "nation"
}

def old_construct_signals(df, metric_names, naics_codes, brand_df):
    """
    Construct Zip Code level signals.

    In its current form, we prepare the following signals

    - bars_visit_num defined as:
        number of visits by day in bars (naics_code = 722410)
    - restaurants_visit_num defined as:
        number of visits by day in restaurants (naics_code = 722511)

    Documentation for the weekly patterns metrics:
    https://docs.safegraph.com/docs/weekly-patterns

    Parameters
    ----------
    df: pd.DataFrame
        dataframe with raw visit number for different places with unique
        safegraph_place_id
    metric_names: List[str]
        Names of metrics to be exported.
    naics_codes: List[int]
        naics codes for bars and restaurants
    brand_df: pd.DataFrame
        mapping info from naics_code to safegraph_brand_id

    Returns
    -------
    dict{key(str): value(pd.DataFrame)}
        Keys are metrics
        Values are Dataframes with columns: timestamp, Zip Codes, {metric}_num.
    """
    result_dfs = {}
    for metric, naics_code in zip(metric_names, naics_codes):
        # Bar or restaurants
        selected_brand_id = brand_df.loc[
                brand_df["naics_code"] == naics_code, "safegraph_brand_id"]
        filtered_df = df[df["safegraph_brand_ids"].isin(selected_brand_id)]
        metric_count_name = "_".join([metric, "num"])

        # Create wide df of visit counts for each day of week, along with zip and date info
        # The counts end up as object by default, so explicitly cast as ints
        visits_df = pd.DataFrame(
                filtered_df["visits_by_day"].str[1:-1].str.split(",").tolist(),
                index=filtered_df.index).astype("int")
        visits_df["zip"] = filtered_df["postal_code"]
        # Just keep date only
        visits_df["start_date"] = pd.to_datetime(filtered_df["date_range_start"], utc=True)
        visits_df["start_date"] = visits_df["start_date"].dt.normalize().dt.tz_localize(None)

        # Turn df into long format and calculate actual dates from start_date and day of week
        visits_long = visits_df.melt(
                id_vars=["zip", "start_date"],
                var_name="day_of_week",
                value_name=metric_count_name)
        visits_long[metric_count_name] = visits_long[metric_count_name].astype("int")
        day_offsets = pd.to_timedelta(visits_long["day_of_week"], "d")
        visits_long["timestamp"] = visits_long["start_date"] + day_offsets
        visits_long.drop(["start_date", "day_of_week"], axis=1, inplace=True)

        # Aggregate sum across same timestamps and zips
        result_dfs[metric] = visits_long.groupby(
                ["timestamp", "zip"]).sum().reset_index()

    # Can have ~30k visits in restaurants missed in those zips in one week.
    # only ~200 visits missed for bars.
    return result_dfs

def construct_signals(df, metric_name):
    """
    Construct Zip Code level signals for some particular metric.

    In its current form, we prepare the following signals

    - bars_visit_num defined as:
        number of visits by day in bars (naics_code = 722410)
    - restaurants_visit_num defined as:
        number of visits by day in restaurants (naics_code = 722511)

    Documentation for the weekly patterns metrics:
    https://docs.safegraph.com/docs/weekly-patterns

    Parameters
    ----------
    df: pd.DataFrame
        dataframe with raw visit number for different places with unique
        safegraph_place_id
    metric_names: List[str]
        Names of metrics to be exported.
    naics_codes: List[int]
        naics codes for bars and restaurants
    brand_df: pd.DataFrame
        mapping info from naics_code to safegraph_brand_id

    Returns
    -------
    pd.DataFrame
        columns: timestamp, Zip Codes, {metric}_num.
    """
    metric_count_name = "_".join([metric_name, "num"])
    # Create wide df of visit counts for each day of week, along with zip and date info
    # The counts end up as object by default, so explicitly cast as ints
    df['visits_cleaned'] = df['visits_by_day'] \
        .map(lambda x: str(x).replace("{'visits': ", "").replace("}", ""))
    visits_df = pd.DataFrame(
            df["visits_cleaned"].str[1:-1].str.split(",").tolist(),
            index=df.index).astype('int')
    visits_df["zip"] = df["postal_code"]
    # Just keep date only
    visits_df["start_date"] = pd.to_datetime(df["date_range_start"], utc=True)
    visits_df["start_date"] = visits_df["start_date"].dt.normalize().dt.tz_localize(None)

    # Turn df into long format and calculate actual dates from start_date and day of week
    visits_long = visits_df.melt(
            id_vars=["zip", "start_date"],
            var_name="day_of_week",
            value_name=metric_count_name)
    visits_long[metric_count_name] = visits_long[metric_count_name].astype("int")
    day_offsets = pd.to_timedelta(visits_long["day_of_week"], "d")
    visits_long["timestamp"] = visits_long["start_date"] + day_offsets
    visits_long.drop(["start_date", "day_of_week"], axis=1, inplace=True)

    # Aggregate sum across same timestamps and zips
    result_df = visits_long.groupby(
            ["timestamp", "zip"]).sum().reset_index()

    return result_df

def aggregate(df, metric, geo_res):
    """
    Aggregate signals to appropriate resolution.

    Parameters
    ----------
    df: pd.DataFrame
        Zip Code-level data with prepared metrics (output of
        construct_metrics().
    metric: str
        Name of metric to be exported.
    geo_resolution: str
        One of ('county', 'hrr, 'msa', 'state', 'hhs', 'nation')

    Returns
    -------
    pd.DataFrame:
        DataFrame with one row per geo_id, with columns for the individual
        signals.
    """
    df = df.copy()
    metric_count_name = "_".join([metric, "num"])
    metric_prop_name = "_".join([metric, "prop"])

    gmpr = GeoMapper()
    geo_key = GEO_KEY_DICT[geo_res]
    df = gmpr.add_population_column(df, "zip")
    df = gmpr.replace_geocode(df, "zip", geo_key, data_cols=[metric_count_name, "population"])

    df[metric_prop_name] = df[metric_count_name] / df["population"] \
                            * INCIDENCE_BASE
    return df.rename({geo_key: "geo_id"}, axis=1)


def handle_dfs(dfs, sensors, metrics, geo_resolutions, export_dir, stats, wips):
    """
    Handle dataframes via create_export_csv.

    Parameters
    ----------
    dfs: dict{key(str): value(pd.DataFrame)}
        Keys are metrics
        Values are Dataframes with columns: timestamp, Zip Codes, {metric}_num.
    sensors: List[str]
        List of (sensor)
    metrics: List[Tuple[str, bool]]
        List of (metric_name, wip).
    geo_resolutions: List[str]
        List of geo resolutions to export the data.
    export_dir: str
        The directory to export files to.
    stats: List[Tuple[datetime, int]]
        List to which we will add (max export date, number of export dates)

    Returns
    -------
    None
    """
    for geo_res, sensor in product(geo_resolutions, sensors):
        for metric, wip in zip(metrics, wips):
            df_export = aggregate(dfs[metric], metric, geo_res)
            df_export["val"] = df_export["_".join([metric, sensor])]
            df_export["se"] = np.nan
            df_export["sample_size"] = np.nan

            if wip:
                metric = "wip_" + metric
            dates = create_export_csv(
                df_export,
                export_dir=export_dir,
                start_date=df_export["timestamp"].min(),
                metric=metric,
                geo_res=geo_res,
                sensor=sensor,
            )
            if len(dates) > 0:
                stats.append((max(dates), len(dates)))

def data(params, day, naics_code, filter_brand = False):
    """Create Dataframe from pull, with optional filter parameter."""
    df = pull(params, day, naics_code)
    if filter_brand:
        brand_url = f'{params["indicator"]["static_file_dir"]} \
            /brand_info/brand_info_202106.csv'
        brand_df = pd.read_csv(brand_url)
        selected_brand_id = brand_df.loc[
            brand_df["naics_code"] == naics_code, "safegraph_brand_id"]
        df = df[df.brands.notna()]
        df['brands'] = df['brands'].apply(lambda x: x[0]['brand_id'])
        filtered_df = df[df["brands"].isin(selected_brand_id)]
    else:
        filtered_df = df
    output_dir = os.path.join(
        params["indicator"]["raw_data_dir"],
        "api",
        "reference_date",
        day.strftime("%Y%m%d"),
        "download_date",
    )
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    filtered_df.to_csv(os.path.join(output_dir, f"{date.today()}_{naics_code}.csv"))
    return filtered_df

def process(day, sensors, metrics, geo_resolutions, export_dir, stats, params, logger):
    """
    Process an input census block group-level CSV and export it.

    Parameters
    ----------
    day: Datetime
        Day of query
    sensors: List[str]
        List of (sensor)
    metrics: List[Tuple[str, bool]]
        List of (metric_name, wip).
    geo_resolutions: List[str]
        List of geo resolutions to export the data.
    export_dir: str
        The directory to export files to.
    stats: List[Tuple[datetime, int]]
        List to which we will add (max export date, number of export dates)
    params:
        Params stored in json file.
    logger: logging.Logger
        The structured logger.

    Returns
    -------
    None
    """
    metric_names, naics_codes, wips = (list(x) for x in zip(*metrics))
    result_dfs = {}
    for i, naics_code in enumerate(naics_codes):
        metric_name = metric_names[i]
        df = data(params, day, naics_code)
        dfs = construct_signals(df, metric_names[i])
        result_dfs[metric_name] = dfs
        logger.info(f"Finished pulling data for {day}, {metric_names[i]}")
    handle_dfs(result_dfs, sensors, metric_names, geo_resolutions, export_dir, stats, wips)

def process_s3(fname, sensors, metrics, geo_resolutions,
               export_dir, brand_df, stats, logger):
    """
    Process an input census block group-level CSV and export it.

    Assumes that the input file has _only_ one date of data.

    Parameters
    ----------
    fname: str
        Input filename.
    sensors: List[str]
        List of (sensor)
    metrics: List[Tuple[str, bool]]
        List of (metric_name, wip).
    geo_resolutions: List[str]
        List of geo resolutions to export the data.
    export_dir: str
        The directory to export files to.
    brand_df: pd.DataFrame
        mapping info from naics_code to safegraph_brand_id
    stats: List[Tuple[datetime, int]]
        List to which we will add (max export date, number of export dates)
    logger: logging.Logger
        The structured logger.

    Returns
    -------
    None
    """
    metric_names, naics_codes, wips = (list(x) for x in zip(*metrics))
    used_cols = [
            "safegraph_brand_ids",
            "visits_by_day",
            "date_range_start",
            "date_range_end",
            "postal_code",
            ]

    if ".csv.gz" in fname:
        df = pd.read_csv(fname,
                         usecols=used_cols,
                         parse_dates=["date_range_start", "date_range_end"])
        dfs = old_construct_signals(df, metric_names, naics_codes, brand_df)
        logger.info("Finished pulling data.", filename=fname)
    else:
        files = glob.glob(f'{fname}/**/*.csv.gz', recursive=True)
        dfs_dict = {"bars_visit": [], "restaurants_visit": []}
        for fn in files:
            df = pd.read_csv(fn,
                         usecols=used_cols,
                         parse_dates=["date_range_start", "date_range_end"])
            dfs = old_construct_signals(df, metric_names, naics_codes, brand_df)
            dfs_dict["bars_visit"].append(dfs["bars_visit"])
            dfs_dict["restaurants_visit"].append(dfs["restaurants_visit"])
        dfs = {}
        dfs["bars_visit"] = pd.concat(dfs_dict["bars_visit"]
            ).groupby(["timestamp", "zip"]).sum().reset_index()
        dfs["restaurants_visit"] = pd.concat(dfs_dict["restaurants_visit"]
            ).groupby(["timestamp", "zip"]).sum().reset_index()
    logger.info("Finished pulling data.", filename=fname)
    for geo_res, sensor in product(geo_resolutions, sensors):
        for metric, _ in zip(metric_names, wips):
            logger.info("Generating signal and exporting to CSV",
                        geo_res=geo_res, metric=metric, sensor=sensor)
            df_export = aggregate(dfs[metric], metric, geo_res)
            df_export["val"] = df_export["_".join([metric, sensor])]
            df_export["se"] = np.nan
            df_export["sample_size"] = np.nan
    handle_dfs(dfs, sensors, metrics, geo_resolutions, export_dir, stats, wips)
