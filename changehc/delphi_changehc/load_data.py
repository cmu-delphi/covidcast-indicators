"""
Load CHC data.

Author: Aaron Rumack
Created: 2020-10-14
"""

# third party
import pandas as pd

# first party
from .config import Config


def load_denom_data(denom_filepath, dropdate, base_geo):
    """Load in and set up denominator data.

    Args:
        denom_filepath: path to the aggregated denominator data
        dropdate: data drop date (datetime object)
        base_geo: base geographic unit before aggregation ('fips')

    Returns:
        cleaned denominator dataframe
    """
    assert base_geo == "fips", "base unit must be 'fips'"

    denom_suffix = denom_filepath.split("/")[-1].split(".")[0][9:]
    assert denom_suffix == "All_Outpatients_By_County"
    denom_filetype = denom_filepath.split("/")[-1].split(".")[1]
    assert denom_filetype == "dat"

    denom_data = pd.read_csv(
        denom_filepath,
        sep="|",
        header=None,
        names=Config.DENOM_COLS,
        dtype=Config.DENOM_DTYPES,
    )

    denom_data[Config.DATE_COL] = \
        pd.to_datetime(denom_data[Config.DATE_COL],errors="coerce")

    # restrict to start and end date
    denom_data = denom_data[
        (denom_data[Config.DATE_COL] >= Config.FIRST_DATA_DATE) &
        (denom_data[Config.DATE_COL] < dropdate)
        ]

    # counts between 1 and 3 are coded as "3 or less", we convert to 1
    denom_data[Config.DENOM_COL][
        denom_data[Config.DENOM_COL] == "3 or less"
        ] = "1"
    denom_data[Config.DENOM_COL] = denom_data[Config.DENOM_COL].astype(int)

    assert (
        (denom_data[Config.DENOM_COL] >= 0).all().all()
    ), "Denominator counts must be nonnegative"

    # aggregate age groups (so data is unique by date and base geography)
    denom_data = denom_data.groupby([base_geo, Config.DATE_COL]).sum()
    denom_data.dropna(inplace=True)  # drop rows with any missing entries

    return denom_data

def load_covid_data(covid_filepath, dropdate, base_geo):
    """Load in and set up denominator data.

    Args:
        covid_filepath: path to the aggregated covid data
        dropdate: data drop date (datetime object)
        base_geo: base geographic unit before aggregation ('fips')

    Returns:
        cleaned denominator dataframe
    """
    assert base_geo == "fips", "base unit must be 'fips'"

    covid_suffix = covid_filepath.split("/")[-1].split(".")[0][9:]
    assert covid_suffix == "Covid_Outpatients_By_County"
    covid_filetype = covid_filepath.split("/")[-1].split(".")[1]
    assert covid_filetype == "dat"

    covid_data = pd.read_csv(
        covid_filepath,
        sep="|",
        header=None,
        names=Config.COVID_COLS,
        dtype=Config.COVID_DTYPES,
        parse_dates=[Config.DATE_COL]
    )

    covid_data[Config.DATE_COL] = \
        pd.to_datetime(covid_data[Config.DATE_COL],errors="coerce")

    # restrict to start and end date
    covid_data = covid_data[
        (covid_data[Config.DATE_COL] >= Config.FIRST_DATA_DATE) &
        (covid_data[Config.DATE_COL] < dropdate)
        ]

    # counts between 1 and 3 are coded as "3 or less", we convert to 1
    covid_data[Config.COVID_COL][
        covid_data[Config.COVID_COL] == "3 or less"
        ] = "1"
    covid_data[Config.COVID_COL] = covid_data[Config.COVID_COL].astype(int)

    assert (
        (covid_data[Config.COVID_COL] >= 0).all().all()
    ), "COVID counts must be nonnegative"

    # aggregate age groups (so data is unique by date and base geography)
    covid_data = covid_data.groupby([base_geo, Config.DATE_COL]).sum()
    covid_data.dropna(inplace=True)  # drop rows with any missing entries

    return covid_data


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
    denom_data = load_denom_data(denom_filepath, dropdate, base_geo)
    covid_data = load_covid_data(covid_filepath, dropdate, base_geo)

    # merge data
    data = denom_data.merge(covid_data, how="outer", left_index=True, right_index=True)
    assert data.isna().all(axis=1).sum() == 0, "entire row is NA after merge"

    # calculate combined numerator and denominator
    data.fillna(0, inplace=True)
    data["num"] = data[Config.COVID_COL]
    data["den"] = data[Config.DENOM_COL]
    data = data[["num", "den"]]

    return data
