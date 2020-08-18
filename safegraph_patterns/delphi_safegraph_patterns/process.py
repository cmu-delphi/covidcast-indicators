# -*- coding: utf-8 -*-
import re
import glob
from datetime import timedelta

import numpy as np
import pandas as pd

from delphi_utils import create_export_csv

INCIDENCE_BASE = 100000

def construct_signals(df, metric_names, naics_codes, brand_df):
    """
    Construct Zip Code level signals.

    In its current form, we prepare the following signals

    - bars_visit_num defined as:
        number of visits by day in bars (naics_code = 722410)
    - restaurants_visit_num defined as:
        number of visits by day in bars (naics_code = 722511)

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
        Dataframe with columns: timestamp, county_fips, and
        {each metric described above}.
    """
    result_dfs = {}
    for metric, naics_code in zip(metric_names, naics_codes):
        # Bar or restaurances
        selected_brand_id = brand_df.loc[
                brand_df["naics_code"] == naics_code, "safegraph_brand_id"]
        filtered_df = df[df["safegraph_brand_ids"].isin(selected_brand_id)]
        metric_count_name = "_".join([metric, "num"])
        result_dfs[metric] = pd.DataFrame(columns=[
                "timestamp", metric_count_name, "zip"])
        tempt_dfs = []
        for idx in filtered_df.index:
            visits = list(map(int, re.split(r"\[|,|\]",
                        filtered_df.loc[idx]["visits_by_day"])[1:-1]))
            start_date = filtered_df.loc[idx]["date_range_start"].date()

            tempt_df = pd.DataFrame({
                "timestamp": [pd.to_datetime(start_date) + timedelta(days=i) for i in range(7)],
                metric_count_name: visits,
                "zip": filtered_df.loc[idx]["postal_code"]
            })
            tempt_dfs.append(tempt_df)
        result_dfs[metric] = pd.concat(tempt_dfs)
        # Sanity Check
        assert result_dfs[metric][metric_count_name].sum() == \
                                        filtered_df["raw_visit_counts"].sum()
        result_dfs[metric] = result_dfs[metric].groupby(
                                    ["timestamp", "zip"]).sum().reset_index()
    result_df = pd.merge(result_dfs[metric_names[0]],
                         result_dfs[metric_names[1]],
                         on=["timestamp", "zip"], how="outer")
#    problematic_set = set(result_df["zip"]) - set(map_df["zip"])
#    result_df[result_df["zip"].isin(problematic_set)].sum()
    # Can have ~30k visits in restaurants missed in those zips in one week.
    # only ~200 visits missed for bars.
    return result_df

def aggregate(df, metric_names, geo_res, map_df):
    """
    Aggregate signals to appropriate resolution.

    Parameters
    ----------
    df: pd.DataFrame
        Zip Code-level data with prepared metrics (output of
        construct_metrics().
    metric_names: List[str]
        Names of metrics to be exported.
    geo_resolution: str
        One of ('county', 'hrr, 'msa', 'state')
    map_df: pd.DataFrame
        population information and mapping info among different geo levels

    Returns
    -------
    pd.DataFrame:
        DataFrame with one row per geo_id, with columns for the individual
        signals.
    """
    df = df.copy()
    # Add pop info
    df = df.merge(map_df[["zip", geo_res, "population"]], on="zip"
                  ).drop("zip", axis=1)
    df = df.groupby(["timestamp", geo_res]).sum().reset_index()
    # Keep NANs
    df.loc[df["bars_visit_num"] == 0, "bars_visit_num"] = np.nan
    df.loc[df["restaurants_visit_num"] == 0, "restaurants_visit_num"] = np.nan

    for metric in metric_names:
        metric_count_name = "_".join([metric, "num"])
        metric_prop_name = "_".join([metric, "prop"])
        df[metric_prop_name] = df[metric_count_name] / df["population"] \
                                * INCIDENCE_BASE
    return df.rename({geo_res: "geo_id"}, axis=1)

def process(fname, sensors, metrics, geo_resolutions,
            export_dir, brand_df, map_df):
    """
    Process an input census block group-level CSV and export it.  Assumes
    that the input file has _only_ one date of data.

    Parameters
    ----------
    fname: str
        Input filename.
    metrics: List[Tuple[str, bool]]
        List of (metric_name, wip).
    sensors: List[str]
        List of (sensor)
    geo_resolutions: List[str]
        List of geo resolutions to export the data.
    brand_df: pd.DataFrame
        mapping info from naics_code to safegraph_brand_id
    map_df: pd.DataFrame
        population information and mapping info among different geo levels

    Returns
    -------
    None
    """
    metric_names, naics_codes, wips = (list(x) for x in zip(*metrics))
    if ".csv.gz" in fname:
        df = pd.read_csv(fname,
                         parse_dates=["date_range_start", "date_range_end"])
        df = construct_signals(df, metric_names, naics_codes, brand_df)
        print("Finished pulling data from " + fname)
    else:
        files = glob.glob(f'{fname}/**/*.csv.gz', recursive=True)
        dfs = []
        for fn in files:
            df = pd.read_csv(fn,
                         parse_dates=["date_range_start", "date_range_end"])
            df = construct_signals(df, metric_names, naics_codes, brand_df)
            dfs.append(df)
        df = pd.concat(dfs).groupby(["timestamp", "zip"]).sum().reset_index()
        print("Finished pulling data from " + fname)
    for geo_res in geo_resolutions:
        df_export = aggregate(df, metric_names, geo_res, map_df)
        for sensor in sensors:
            for metric, wip in zip(metric_names, wips):
                df_export["val"] = df_export["_".join([metric, sensor])]
                df_export["se"] = np.nan
                df_export["sample_size"] = np.nan

                if wip:
                    metric = "wip_" + metric
                create_export_csv(
                    df_export,
                    export_dir=export_dir,
                    start_date=df["timestamp"].min(),
                    metric=metric,
                    geo_res=geo_res,
                    sensor=sensor,
                )
