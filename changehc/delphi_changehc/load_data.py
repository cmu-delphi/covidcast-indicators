"""
Load CHC data.

Author: Aaron Rumack
Created: 2020-10-14
"""

# third party
import pandas as pd

# first party
from .config import Config


def load_chng_data(filepath, dropdate, base_geo,
                   col_names, col_types, counts_col):
    """Load in and set up daily count data from Change.

    Args:
        filepath: path to aggregated data
        dropdate: data drop date (datetime object)
        base_geo: base geographic unit before aggregation ('fips')
        col_names: column names of data
        col_types: column types of data
        counts_col: name of column containing counts

    Returns:
        cleaned dataframe
    """
    assert base_geo == "fips", "base unit must be 'fips'"
    count_flag = False
    date_flag = False
    geo_flag = False
    for n in col_names:
        if n == counts_col:
            count_flag = True
        elif n == Config.DATE_COL:
            date_flag = True
        elif n == "fips":
            geo_flag = True
    assert count_flag, "counts_col must be present in col_names"
    assert date_flag, "'%s' must be present in col_names"%(Config.DATE_COL)
    assert geo_flag, "'fips' must be present in col_names"

    data = pd.read_csv(
        filepath,
        sep="|",
        header=None,
        names=col_names,
        dtype=col_types,
    )

    data[Config.DATE_COL] = \
        pd.to_datetime(data[Config.DATE_COL],errors="coerce")

    # restrict to start and end date
    data = data[
        (data[Config.DATE_COL] >= Config.FIRST_DATA_DATE) &
        (data[Config.DATE_COL] < dropdate)
        ]

    # counts between 1 and 3 are coded as "3 or less", we convert to 1
    data[counts_col][
        data[counts_col] == "3 or less"
        ] = "1"
    data[counts_col] = data[counts_col].astype(int)

    assert (
        (data[counts_col] >= 0).all().all()
    ), "Counts must be nonnegative"

    # aggregate age groups (so data is unique by date and base geography)
    data = data.groupby([base_geo, Config.DATE_COL]).sum()
    data.dropna(inplace=True)  # drop rows with any missing entries

    return data


def load_combined_data(denom_filepath, covid_filepath, dropdate, base_geo):
    """Load in denominator and covid data, and combine them.

    Args:
        denom_filepath: path to the aggregated denominator data
        covid_filepath: path to the aggregated covid data
        dropdate: data drop date (datetime object)
        base_geo: base geographic unit before aggregation ('fips')

    Returns:
        combined multiindexed dataframe, index 0 is geo_base, index 1 is date
    """
    assert base_geo == "fips", "base unit must be 'fips'"

    # load each data stream
    denom_data = load_chng_data(denom_filepath, dropdate, base_geo,
                     Config.DENOM_COLS, Config.DENOM_DTYPES, Config.DENOM_COL)
    covid_data = load_chng_data(covid_filepath, dropdate, base_geo,
                     Config.COVID_COLS, Config.COVID_DTYPES, Config.COVID_COL)

    # merge data
    data = denom_data.merge(covid_data, how="outer", left_index=True, right_index=True)
    assert data.isna().all(axis=1).sum() == 0, "entire row is NA after merge"

    # calculate combined numerator and denominator
    data.fillna(0, inplace=True)
    data["num"] = data[Config.COVID_COL]
    data["den"] = data[Config.DENOM_COL]
    data = data[["num", "den"]]

    return data


def load_cli_data(denom_filepath, flu_filepath, mixed_filepath, flu_like_filepath,
                  covid_like_filepath, dropdate, base_geo):
    """Load in denominator and covid-like data, and combine them.

    Args:
        denom_filepath: path to the aggregated denominator data
        flu_filepath: path to the aggregated flu data
        mixed_filepath: path to the aggregated mixed data
        flu_like_filepath: path to the aggregated flu-like data
        covid_like_filepath: path to the aggregated covid-like data
        dropdate: data drop date (datetime object)
        base_geo: base geographic unit before aggregation ('fips')

    Returns:
        combined multiindexed dataframe, index 0 is geo_base, index 1 is date
    """
    assert base_geo == "fips", "base unit must be 'fips'"

    # load each data stream
    denom_data = load_chng_data(denom_filepath, dropdate, base_geo,
                     Config.DENOM_COLS, Config.DENOM_DTYPES, Config.DENOM_COL)
    flu_data = load_chng_data(flu_filepath, dropdate, base_geo,
                     Config.FLU_COLS, Config.FLU_DTYPES, Config.FLU_COL)
    mixed_data = load_chng_data(mixed_filepath, dropdate, base_geo,
                     Config.MIXED_COLS, Config.MIXED_DTYPES, Config.MIXED_COL)
    flu_like_data = load_chng_data(flu_like_filepath, dropdate, base_geo,
                     Config.FLU_LIKE_COLS, Config.FLU_LIKE_DTYPES, Config.FLU_LIKE_COL)
    covid_like_data = load_chng_data(covid_like_filepath, dropdate, base_geo,
                     Config.COVID_LIKE_COLS, Config.COVID_LIKE_DTYPES, Config.COVID_LIKE_COL)

    # merge data
    data = denom_data.merge(flu_data, how="outer", left_index=True, right_index=True)
    data = data.merge(mixed_data, how="outer", left_index=True, right_index=True)
    data = data.merge(flu_like_data, how="outer", left_index=True, right_index=True)
    data = data.merge(covid_like_data, how="outer", left_index=True, right_index=True)
    assert data.isna().all(axis=1).sum() == 0, "entire row is NA after merge"

    # calculate combined numerator and denominator
    data.fillna(0, inplace=True)
    data["num"] = -data[Config.FLU_COL] + data[Config.MIXED_COL] + data[Config.FLU_LIKE_COL]
    data["num"] = data["num"].clip(lower=0)
    data["num"] = data["num"] + data[Config.COVID_LIKE_COL]
    data["den"] = data[Config.DENOM_COL]
    data = data[["num", "den"]]

    return data
