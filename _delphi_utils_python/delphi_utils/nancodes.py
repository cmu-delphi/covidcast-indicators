"""Unified not-a-number codes for CMU Delphi codebase."""

from enum import IntEnum
import pandas as pd


class Nans(IntEnum):
    """An enum of not-a-number codes for the indicators.

    See the descriptions here: https://cmu-delphi.github.io/delphi-epidata/api/missing_codes.html
    """

    NOT_MISSING = 0
    NOT_APPLICABLE = 1
    REGION_EXCEPTION = 2
    CENSORED = 3
    DELETED = 4
    OTHER = 5


def add_default_nancodes(df: pd.DataFrame):
    """Add some default nancodes to the dataframe.

    This method sets the `"missing_val"` column to NOT_MISSING whenever the
    `"val"` column has `isnull()` as `False`; if `isnull()` is `True`, then it
    sets `"missing_val"` to `OTHER`. It also sets both the `"missing_se"` and
    `"missing_sample_size"` columns to `NOT_APPLICABLE`.

    Returns
    -------
    pd.DataFrame
    """
    # Default missingness codes
    df["missing_val"] = Nans.NOT_MISSING
    df["missing_se"] = Nans.NOT_APPLICABLE
    df["missing_sample_size"] = Nans.NOT_APPLICABLE

    # Mark any remaining nans with unknown
    remaining_nans_mask = df["val"].isnull()
    df.loc[remaining_nans_mask, "missing_val"] = Nans.OTHER
    return df
