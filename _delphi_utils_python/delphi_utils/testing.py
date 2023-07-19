"""Common utilities for testing functions."""
from typing import Any, Dict
import pandas as pd


def check_valid_dtype(dtype):
    """Check if a dtype is a valid Pandas type."""
    try:
        pd.api.types.pandas_dtype(dtype)
    except TypeError as e:
        raise ValueError(f"Invalid dtype {dtype}") from e


def set_df_dtypes(df: pd.DataFrame, dtypes: Dict[str, Any]) -> pd.DataFrame:
    """Set the dataframe column datatypes."""
    for d in dtypes.values():
        check_valid_dtype(d)

    df = df.copy()
    for k, v in dtypes.items():
        if k in df.columns:
            df[k] = df[k].astype(v)
    return df
