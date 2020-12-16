"""Functions for mapping geographic regions."""

import pandas as pd

from .constants import NAN_VALUE


def pull_data() -> pd.DataFrame:
    """
    Pull HHS data.

    NOTE: Currently these commands are all just for local testing while API is not online yet.

    Returns
    -------
    DataFrame of HHS data.
    """
    df = pd.read_csv("delphi_hhs_facilities/sample_data.csv",
                     dtype={"fips_code": str, "zip": str},
                     nrows=10000, na_values=NAN_VALUE)
    df["timestamp"] = pd.to_datetime(df["collection_week"])
    return df
