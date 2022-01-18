"""Contains geographic mapping tools."""
from itertools import product
from functools import reduce

import pandas as pd

from delphi_utils import GeoMapper
from .constants import (AGE_GROUPS, MIN_OBS)

DATA_COLS = ['totalTest', 'numUniqueDevices', 'positiveTest']
GMPR = GeoMapper()  # Use geo utils
GEO_KEY_DICT = {
        "county": "fips",
        "msa": "msa",
        "hrr": "hrr",
        "state": "state_id",
        "nation": "nation",
        "hhs": "hhs"
}

def geo_map(geo_res, df):
    """Map a geocode to a new value."""
    data = df.copy()
    geo_key = GEO_KEY_DICT[geo_res]
    # Add population for each zipcode
    data = GMPR.add_population_column(data, "zip")
    # zip -> geo_res
    data_cols = ["population"]
    for col, agegroup in product(DATA_COLS, AGE_GROUPS):
        data_cols.append("_".join([col, agegroup]))

    data = GMPR.replace_geocode(
        data, from_code="zip", new_code=geo_key, date_col = "timestamp",
        data_cols=data_cols)
    if geo_res in ["state", "hhs", "nation"]:
        return data, geo_key
    # Add parent state
    data = add_parent_state(data, geo_res, geo_key)
    return data, geo_key

def add_megacounties(data, smooth=False):
    """Add megacounties to county level report."""
    assert "fips" in data.columns # Make sure the data is at county level

    # For raw signals, the threshold is MIN_OBS
    # For smoothed signals, the threshold is MIN_OBS/2
    if smooth:
        threshold_visits = MIN_OBS/2
    else:
        threshold_visits = MIN_OBS
    pdList = []
    for agegroup in AGE_GROUPS:
        data_cols = [f"{col}_{agegroup}" for col in DATA_COLS]
        df = GMPR.fips_to_megacounty(data[data_cols + ["timestamp", "fips"]],
                                     threshold_visits, 1, fips_col="fips",
                                     thr_col=f"totalTest_{agegroup}",
                                     date_col="timestamp")
        df.rename({"megafips": "fips"}, axis=1, inplace=True)
        megacounties = df[df.fips.str.endswith("000")]
        pdList.append(megacounties)
    mega_df = reduce(lambda x, y: pd.merge(
        x, y, on = ["timestamp", "fips"]), pdList)

    return pd.concat([data, mega_df])

def add_parent_state(data, geo_res, geo_key):
    """
    Add parent state column to DataFrame.

    - map from msa/hrr to state, going by the state with the largest
      population (since a msa/hrr may span multiple states)
    - map from county to the corresponding state
    """
    fips_to_state = GMPR.get_crosswalk(from_code="fips", to_code="state")
    if geo_res == "county":
        mix_map = fips_to_state[["fips", "state_id"]]  # pylint: disable=unsubscriptable-object
    else:
        fips_to_geo_res = GMPR.get_crosswalk(from_code="fips", to_code=geo_res)
        mix_map = fips_to_geo_res[["fips", geo_res]].merge(
                fips_to_state[["fips", "state_id"]],  # pylint: disable=unsubscriptable-object
                on="fips",
                how="inner")
        mix_map = GMPR.add_population_column(mix_map, "fips").groupby(
                geo_res).max().reset_index().drop(
                ["fips", "population"], axis = 1)
    # Merge the info of parent state to the data
    data = data.merge(mix_map, how="left", on=geo_key).drop(
        columns=["population"]).dropna()
    data = data.groupby(["timestamp", geo_key, "state_id"]).sum().reset_index()
    return data
