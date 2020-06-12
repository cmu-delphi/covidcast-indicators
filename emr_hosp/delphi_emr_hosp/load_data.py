"""
Load hospitalization data.

Author: Maria Jahja
Created: 2020-06-12
"""

# third party
import pandas as pd

# first party
from .config import Config


def load_emr_data(emr_filepath, dropdate, base_geo):
    """Load in and set up EMR data.

    Args:
      emr_filepath: path to the aggregated EMR data
      dropdate: data drop date (datetime object)
      base_geo: base geographic unit before aggregation (either 'fips' or 'hrr')

    Returns:
      cleaned emr dataframe
    """
    emr_data = pd.read_csv(
        emr_filepath,
        usecols=Config.EMR_DTYPES.keys(),
        dtype=Config.EMR_DTYPES,
        parse_dates=[Config.EMR_DATE_COL]
    )

    # standardize naming
    emr_data.rename(columns=Config.EMR_RENAME_COLS, inplace=True)

    # restrict to start and end date
    emr_data = emr_data[
        (emr_data[Config.DATE_COL] >= Config.FIRST_DATA_DATE) &
        (emr_data[Config.DATE_COL] < dropdate)
        ]

    assert (
        (emr_data[Config.EMR_COUNT_COLS] >= 0).all().all()
    ), "EMR counts must be nonnegative"

    # aggregate age groups (so data is unique by date and base geography)
    emr_data = emr_data.groupby([base_geo, "date"]).sum()
    emr_data.dropna(inplace=True)  # drop rows with any missing entries

    return emr_data


def load_claims_data(claims_filepath, dropdate, base_geo):
    """Load in and set up claims data.

    Args:
      claims_filepath: path to the aggregated claims data
      dropdate: data drop date (datetime object)
      base_geo: base geographic unit before aggregation (either 'fips' or 'hrr')

    Returns:
      cleaned claims dataframe
    """
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
