# -*- coding: utf-8 -*-
"""Functions to load data from GoogleHeathTrends.

Most of the logic is to deal with the caching files.
"""
import time

import numpy as np
import pandas as pd
from googleapiclient.discovery import build

DISCOVERY_URL = "https://www.googleapis.com/discovery/v1/apis/trends/v1beta/rest"
TERMS_IDS = "anosmia_ms"
TERMS = ["/m/0m7pl", "why cant i smell or taste", "loss of smell", "loss of taste"]


class GoogleHealthTrends:
    """Small class to query the Google Health Trends API.

    Parameters
    ----------
    ght_key: str
        The Google Health Trends API key as a string.
    """

    def __init__(self, ght_key: str):
        self.key = ght_key
        self.service = build(
            serviceName="trends",
            version="v1beta",
            developerKey=ght_key,
            discoveryServiceUrl=DISCOVERY_URL,
            cache_discovery=False,
        )

    def query(self, start_date: str, end_date: str, geo_id, dma=False) -> dict:
        """Query the API.

        Parameters
        ----------
        start_date: str
            start date as a string formated as "YYYY-MM-DD"
        end_date: str
            end date as a string formated as "YYYY-MM-DD"
        geo_id: str
            geo_id of the region to grab; either a numeric value (DMA) or a two
            letter state abbreviation
        dma: bool
            is the `geo_id` a DMA code? Otherwise, assumes that the code is a
            two-letter state code.

        Returns
        -------
        pd.DataFrame
            the returned JSON object as a Python dictionary
        """
        params = {
            "time_startDate": start_date,
            "time_endDate": end_date,
            "timelineResolution": "day",
            "terms": TERMS,
        }

        if dma:
            params["geoRestriction_dma"] = geo_id
        else:
            params["geoRestriction_region"] = "US-" + geo_id

        time.sleep(1)
        data = self.service.getTimelinesForHealth(**params).execute()

        return data


def get_counts_states(
    ght: GoogleHealthTrends,
    start_date: str,
    end_date: str,
    static_dir: str,
    cache_dir: str,
) -> pd.DataFrame:
    """Returns data from Google Trends Health API for all states in a data range.

    Parameters
    ----------
    ght: GoogleHealthTrends
        an initialized GoogleHealthTrends object; must have a valid API key
        unless all of the required data has already been cached
    geo_id: str
        geo_id of the region to grab; either a numeric value (DMA) or a two
        letter state abbreviation
    start_date: str
        start date as a string formated as "YYYY-MM-DD"
    end_date: str
        end date as a string formated as "YYYY-MM-DD"
    static_dir: str
        path to location where static metadata files are stored
    cache_dir: str
        path to location where cached CSV files are stored

    Returns
    -------
    pd.DataFrame
        a data frame with columns "geo_id", "timestamp", and "val"
    """
    state_list = np.loadtxt(f"{static_dir}/Canonical_STATE.txt", dtype=str)

    state_df_list = []
    for state in state_list:
        state_df_list += [
            _get_counts_geoid(
                ght, state, start_date, end_date, dma=False, cache_dir=cache_dir
            )
        ]

    state_df = pd.concat(state_df_list)
    state_df["geo_id"] = state_df["geo_id"].str.lower()

    return state_df


def get_counts_dma(
    ght: GoogleHealthTrends,
    start_date: str,
    end_date: str,
    static_dir: str,
    cache_dir: str,
) -> pd.DataFrame:
    """Returns data from Google Trends Health API for all DMA regions in a data range.

    Parameters
    ----------
    ght: GoogleHealthTrends
        an initialized GoogleHealthTrends object; must have a valid API key
        unless all of the required data has already been cached
    geo_id: str
        geo_id of the region to grab; either a numeric value (DMA) or a two
        letter state abbreviation
    start_date: str
        start date as a string formated as "YYYY-MM-DD"
    end_date: str
        end date as a string formated as "YYYY-MM-DD"
    static_dir: str
        path to location where static metadata files are stored
    cache_dir: str
        path to location where cached CSV files are stored

    Returns
    -------
    pd.DataFrame
        a data frame with columns "geo_id", "timestamp", and "val"
    """
    dma_list = np.loadtxt(f"{static_dir}/Canonical_DMA.txt", dtype=str)

    dma_df_list = []
    for dma in dma_list:
        dma_df_list += [
            _get_counts_geoid(
                ght, dma, start_date, end_date, dma=True, cache_dir=cache_dir
            )
        ]

    return pd.concat(dma_df_list)


def _get_counts_geoid(
    ght: GoogleHealthTrends,
    geo_id: str,
    start_date: str,
    end_date: str,
    dma: bool,
    cache_dir: str,
) -> pd.DataFrame:
    """Given a GeoID and date range, return data frame of counts from Google Trends Health API

    Parameters
    ----------
    ght: GoogleHealthTrends
        an initialized GoogleHealthTrends object; must have a valid API key
        unless all of the required data has already been cached
    geo_id: str
        geo_id of the region to grab; either a numeric value (DMA) or a two
        letter state abbreviation
    start_date: str
        start date as a string formated as "YYYY-MM-DD"
    end_date: str
        end date as a string formated as "YYYY-MM-DD"
    dma: bool
        is the `geo_id` a DMA code? Otherwise, assumes that the code is a
        two-letter state code.
    cache_dir: str
        path to location where cached CSV files are stored

    Returns
    -------
    pd.DataFrame
        a data frame with columns "geo_id", "timestamp", and "val"
    """

    dt = _load_cached_file(geo_id, cache_dir).sort_values("timestamp")

    output_dates = set(pd.date_range(start_date, end_date).to_native_types())
    cache_dates = set(dt["timestamp"].values)
    req_dates = list(output_dates - cache_dates)

    if req_dates:
        sdate = min(req_dates)
        edate = max(req_dates)
        new_data = _api_data_to_df(
            ght.query(start_date=sdate, end_date=edate, geo_id=geo_id, dma=dma),
            geo_id=geo_id,
        )
        new_data = new_data[new_data["timestamp"].isin(req_dates)]
        dt = dt.append(new_data).sort_values("timestamp")
        dt = dt.drop_duplicates(subset="timestamp")
        _write_cached_file(dt, geo_id, cache_dir)
        dt = _load_cached_file(geo_id, cache_dir)

    dt = dt[dt["timestamp"].isin(output_dates)]
    return dt


def _api_data_to_df(data: dict, geo_id: str) -> pd.DataFrame:
    """Converts raw dictionary from API call to a data frame object.

    Parameters
    ----------
    data: dict
        a dictionary representation of the JSON data pulled from the Google
        Healths Trends API
    geo_id: str
        geo_id of the region to grab; either a numeric value (DMA) or a two
        letter state abbreviation

    Returns
    -------
    pd.DataFrame
        a data frame with columns "geo_id", "timestamp", and "val"
    """
    pairs = []
    for term in data["lines"]:
        for pt in term["points"]:
            pairs += [(pt["date"], pt["value"])]
    df = pd.DataFrame(
        {
            "geo_id": geo_id,
            "timestamp": pd.to_datetime([p[0] for p in pairs]).strftime("%Y-%m-%d"),
            "val": [p[1] for p in pairs],
        }
    )
    df = df.groupby(["geo_id", "timestamp"]).sum().reset_index()
    return df


def _load_cached_file(geo_id: str, cache_dir: str) -> pd.DataFrame:
    """Read cached data file for a given GeoID.

    Parameters
    ----------
    geo_id: str
        geo_id of the region to grab; either a numeric value (DMA) or a two
        letter state abbreviation
    cache_dir: str
        path to location where cached CSV files are stored

    Returns
    -------
    pd.DataFrame
    """
    try:
        fn_cache = f"./{cache_dir}/Data_{geo_id}_{TERMS_IDS}.csv"
        return pd.read_csv(fn_cache)
    except FileNotFoundError:
        return pd.DataFrame({"geo_id": [], "timestamp": [], "val": []})


def _write_cached_file(df: pd.DataFrame, geo_id: str, cache_dir: str):
    """Save a data frame as a CSV cache file for later usage.

    Parameters
    ----------
    df: pd.DataFrame
        ...
    geo_id: str
        geo_id of the region to grab; either a numeric value (DMA) or a two
        letter state abbreviation
    cache_dir: str
        path to location where cached CSV files are stored
    """
    fn_cache = f"./{cache_dir}/Data_{geo_id}_{TERMS_IDS}.csv"
    df.to_csv(fn_cache, index=False)
