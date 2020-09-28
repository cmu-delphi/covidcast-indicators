"""
Load hospitalization data.

Author: Maria Jahja
Created: 2020-09-27

"""

# third party
import pandas as pd

# first party
from .config import Config


def load_claims_data(claims_filepath, dropdate, base_geo):
    """
    Load in and set up claims data.

    Args:
        claims_filepath: path to the aggregated claims data
        dropdate: data drop date (datetime object)
        base_geo: base geographic unit before aggregation (either 'fips' or 'hrr')

    Returns:
        cleaned claims dataframe
    """
    assert base_geo in ["fips", "hrr"], "base unit must be either 'fips' or 'hrr'"

    claims_data = pd.read_csv(
        claims_filepath,
        usecols=Config.CLAIMS_DTYPES.keys(),
        dtype=Config.CLAIMS_DTYPES,
        parse_dates=[Config.CLAIMS_DATE_COL],
    )

    # standardize naming
    claims_data.rename(columns=Config.CLAIMS_RENAME_COLS, inplace=True)

    # restrict to start and end date
    claims_data = claims_data[
        (claims_data[Config.DATE_COL] >= Config.FIRST_DATA_DATE) &
        (claims_data[Config.DATE_COL] < dropdate)
        ]

    assert (
        (claims_data[Config.CLAIMS_COUNT_COLS] >= 0).all().all()
    ), "Claims counts must be nonnegative"

    # aggregate age groups (so data is unique by date and base geography)
    claims_data = claims_data.groupby([base_geo, "date"]).sum()
    claims_data.dropna(inplace=True)  # drop rows with any missing entries

    return claims_data


def load_data(input_filepath, dropdate, base_geo):
    """
    Load in claims data, and combine them.

    Args:
        input_filepath: path to the aggregated data
        dropdate: data drop date (datetime object)
        base_geo: base geographic unit before aggregation (either 'fips' or 'hrr')

    Returns:
        combined multiindexed dataframe, index 0 is geo_base, index 1 is date
    """
    assert base_geo in ["fips", "hrr"], "base unit must be either 'fips' or 'hrr'"

    # load data stream
    data = load_claims_data(input_filepath, dropdate, base_geo)

    # rename numerator and denominator
    data.fillna(0, inplace=True)
    data["num"] = data["Covid_like"]
    data["den"] = data["Denominator"]
    data = data[['num', 'den']]
    data.reset_index(inplace=True)

    return data
