"""
Generate support dataframes for backfill.

"""
# standard packages
from datetime import datetime, timedelta

# third party
import numpy as np
import pandas as pd
from delphi_utils import GeoMapper

# first party
from .config import Config
from .data_tools import (date_range, load_chng_data, get_weekofmonth)

gmpr = GeoMapper()

exclude_states = set(["as", "mp", "gu"]) # exclude states with few datapoints

def get_top200_populated_fips(cache_dir):
    """Generate the list of fips for the most populated counties.

    Args:
       cache_dir: directory to the cache folder

    Returns:
        the set of the top 200 populated fips
    """
    pop_file = pd.read_csv("/".join([cache_dir,
                       gmpr.crosswalk_filepaths["fips"]["pop"].split("/")[1]]),
                       dtype={"fips": str, "pop": int})
    return set(pop_file.sort_values("pop", ascending=False)["fips"][:200])

def generate_pivot_df(filepath, input_cache_dir, data_cache_dir,
                      count_type, dropdate, test=False):
    """Convert the count dataframe into backfill format.

    Return the pivot table of backfill for different geo levels. It has the
    county-level counts as the values, lags for the columns and location-
    reference date as indexes.

    - If the update is NAN, we assume there is 0 addtive count compare to the
    previous update.
    - If the count is NAN before the first non-zero report, we assume there
    is zero count for that location.

    Parameters
    ----------
    filepath: path to aggregated data
    input_cache_dir: the directory to the cache folder for input data
    data_cache_dir: the directory to the cache folder for supporting data
    count_type: "Covid" or "Denom"
    dropdate : the most recent issue date considered
    geo: geographic unit

    Returns
    -------
    result_df: dict
    """
    n_days = 180
    if test:
        date_list = date_range(dropdate, 8)
    else:
        date_list = date_range(dropdate, n_days+1)

    # Slow if date list is long enough
    df = load_chng_data(filepath, input_cache_dir, date_list, count_type,
                        Config.DATA_COLS, Config.DATA_DTYPES, Config.COUNT_COL)

    pivot_df = pd.pivot_table(df, values=Config.COUNT_COL,
                              index=[Config.DATE_COL, Config.GEO_COL],
                              columns="issue_date").reset_index()

    # Fill in missing dates for both refdate and issuedate with NANs
    t_max, t_min = df[Config.DATE_COL].max(), df[Config.DATE_COL].min()
    geo_list = df[Config.GEO_COL].unique()
    refdate_list = pd.date_range(start=t_min, end=t_max, freq='D')
    index_df = pd.MultiIndex.from_product(
        [refdate_list, geo_list], names=[Config.DATE_COL, Config.GEO_COL])
    pivot_df = pivot_df.set_index(
        [Config.DATE_COL, Config.GEO_COL]).reindex(
            index_df, columns=date_list).reset_index()

    pivot_df.iloc[:, 2:] = pivot_df.iloc[:, 2:].fillna(method="ffill", axis=1)

    result_df = {}
    selected_top_200_fips = get_top200_populated_fips(data_cache_dir)
    result_df["fips"] = pivot_df.loc[pivot_df[Config.GEO_COL].isin(
        selected_top_200_fips)].fillna(0)

    result_df["state_id"] = gmpr.replace_geocode(
        result_df["fips"], "fips", "state_id", from_col=Config.GEO_COL,
        new_col="state", date_col=Config.DATE_COL, data_cols=date_list)
    result_df["state_id"].rename({"state": Config.GEO_COL}, axis=1, inplace=True)
    result_df["state_id"] = result_df["state_id"].loc[
        ~result_df["state_id"][Config.GEO_COL].isin(exclude_states)].fillna(0)
    return result_df

def add_value_raw_and_7dav(pivot_df):
    """Prepare the information of raw count and 7-day average of the raw count.

    Parameters
    ----------
    pivot_df : pd.DataFrame
        raw count and 7-day avg of the raw count for each location, reference
        date and issue date.

    Returns
    -------
    dataframe

    """
    #Problematic when computing mean, python treat str(zip) as numbers
    pivot_df["geo_value"] = "s"+pivot_df["geo_value"]
    avg_df = pivot_df.set_index(
        Config.DATE_COL).groupby(
        Config.GEO_COL).rolling(
        7, min_periods=1).mean().reset_index()
    pivot_df["geo_value"] = pivot_df["geo_value"].apply(lambda x:x[1:])
    avg_df["geo_value"] = avg_df["geo_value"].apply(lambda x:x[1:])
    avg_df = pd.melt(avg_df, id_vars=[Config.GEO_COL, Config.DATE_COL],
                          var_name="issue_date", value_name="value_7dav")
    raw_df = pd.melt(pivot_df, id_vars=[Config.GEO_COL, Config.DATE_COL],
                          var_name="issue_date", value_name="value_raw")
    return raw_df.merge(avg_df,
                        on=[Config.GEO_COL,Config.DATE_COL,"issue_date"])

def generate_backfill_df(pivot_df, support_df, dropdate,
                         backfill_type=Config.CHANGE_RATE, ref_lag=1):
    """Generate backfill dataframe for dates in a certain range.

    Args:
       filepath: path to the aggregated data
       cache_dir: the directory to the cache folder
       count_type: type of count, can be COVID or TOTAL
       date_list: list of data drop dates (datetime object)
       geo: geographic unit

    Returns:
        backfill dataframe
    """
    date_list = pivot_df.columns[2:]

    if backfill_type == Config.BACKFILL_FRACTION:
        support_df["lag"] = (support_df["issue_date"] \
                          - support_df[Config.DATE_COL]).dt.days
        anchor_df = support_df.loc[(support_df["lag"] == 60),
                                   [Config.DATE_COL, Config.GEO_COL, "value_raw"]]
        anchor_df = anchor_df.rename({"value_raw": "value_anchor"},
                                     axis=1).drop_duplicates()
        backfill_df = support_df.merge(anchor_df[anchor_df["value_anchor"]!=0],
                                       on=[Config.DATE_COL, Config.GEO_COL])
        backfill_df.loc["value"] = backfill_df["value_raw"] \
            / backfill_df["value_anchor"]

    else:
        for i in range(len(date_list)-1, ref_lag-1, -1):
            cur_date = date_list[i]
            ref_date = date_list[i-ref_lag]
            pivot_df[cur_date] = round(pivot_df[cur_date] - pivot_df[ref_date],
                                6) / pivot_df[ref_date]
            # Ignore two small
            pivot_df.loc[pivot_df[ref_date] < 1, cur_date] = np.nan
        pivot_df.drop(date_list[:ref_lag], axis=1, inplace=True)

        backfill_df = pd.melt(pivot_df, id_vars=[Config.GEO_COL, Config.DATE_COL],
                              var_name="issue_date", value_name="value")
        #Get raw number of counts and 7-day avg of acounts
        backfill_df = backfill_df.merge(
            support_df, how="left",
            on=[Config.DATE_COL, Config.GEO_COL, "issue_date"]).dropna()
        backfill_df["lag"] = (backfill_df["issue_date"] \
                              - backfill_df[Config.DATE_COL]).dt.days

    backfill_df = backfill_df.loc[(backfill_df["lag"] < 60)
                                  & (backfill_df["lag"] >= 0)]

    #Get the covariates
    backfill_df["dayofweek"] = [x.weekday() for x in backfill_df["issue_date"]]
    for i in range(6):
        backfill_df[f"issueD{i}"] = (backfill_df["dayofweek"] == i).astype(int)
    backfill_df["dayofweek2"] = [x.weekday() for x in backfill_df[Config.DATE_COL]]
    for i in range(6):
        backfill_df[f"refD{i}"] = (backfill_df["dayofweek2"] == i).astype(int)
    backfill_df["weekofmonth"] = [get_weekofmonth(x) for x in backfill_df["issue_date"]]
    for i in range(3):
        backfill_df[f"issueW{i}"] = (backfill_df["weekofmonth"] == i).astype(int)
    backfill_df["log_value_7dav"] = backfill_df["value_7dav"].apply(
        lambda x: np.log(x+1))

    return backfill_df.loc[backfill_df["issue_date"] <= dropdate]
