"""Geo-aggregation related functions."""

import numpy as np
import pandas as pd

from delphi_utils import GeoMapper

from .constants import GEO_RESOLUTIONS

gmpr = GeoMapper()

def positivity_rate(x):
    """
    Find Positivity Rate from binomial counts.

    Assumes input sample_size are all > 0.

    Parameters
    ----------
    x: pd.DataFrame
        Columns: pcr_tests_positive, sample_size, ...

    Returns
    -------
    pd.Series
        Positivity Rate of PCR-specimen tests.
    """
    p = x.pcr_tests_positive / x.sample_size

    return p

def std_err(x):
    """
    Find Standard Error of a binomial proportion.

    Assumes input sample_size are all > 0.

    Parameters
    ----------
    x: pd.DataFrame
        Columns: val, sample_size, ...

    Returns
    -------
    pd.Series
        Standard error of the positivity rate of PCR-specimen tests.
    """
    p = x.val
    n = x.sample_size
    return np.sqrt(p * (1 - p) / n)

def geo_map(df: pd.DataFrame, geo_res: str) -> pd.DataFrame:
    """
    Aggregate county-level PCR testing metrics to other geographical levels specified by `geo_res`.

    Parameters
    ----------
    df: pd.DataFrame
        Columns: fips, timestamp, pcr_tests_positive, pcr_tests_total, ...
    geo_res: str
        Geographic resolution to which to aggregate.  Valid options:
        ("county", "state", "msa", "hrr", "hhs", "nation").

    Returns
    -------
    pd.DataFrame
        Dataframe where val is positivity rate and sample_size is total tests.
        Columns: geo_id, timestamp, val, sample_size, se
    """
    if geo_res not in GEO_RESOLUTIONS:
        raise ValueError(f"geo_res must be one of {GEO_RESOLUTIONS}, got '{geo_res}'")

    if (df.pcr_tests_positive > df.pcr_tests_total).any():
        raise ValueError("Found some test positive count greater than the total")

    if (df.pcr_tests_total <= 0).any():
        raise ValueError("Found some test total <= 0")

    if geo_res == "county":
        df = (df
            .rename(columns={
                "fips": "geo_id",
                "pcr_positivity_rate": "val",
                "pcr_tests_total": "sample_size"})
            .assign(se=std_err)
        )

    else:
        # All other geo_res can be used directly with GeoMapper
        if geo_res == "state":
            geo_res = "state_id"

        df = (df
            .loc[:, ["fips", "timestamp", "pcr_tests_positive", "pcr_tests_total"]]
            .pipe(gmpr.replace_geocode, "fips", geo_res, new_col="geo_id",
                date_col="timestamp")
            .rename(columns={"pcr_tests_total": "sample_size"})
            .assign(val=positivity_rate, se=std_err)
            .reset_index()
        )

    return df
