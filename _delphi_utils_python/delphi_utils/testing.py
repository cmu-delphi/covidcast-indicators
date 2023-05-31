from typing import Any, Dict
import pandas as pd


def check_valid_dtype(dtype):
    try:
        pd.api.types.pandas_dtype(dtype)
    except TypeError:
        raise ValueError(f"Invalid dtype {dtype}")


def set_df_dtypes(df: pd.DataFrame, dtypes: Dict[str, Any]) -> pd.DataFrame:
    """Set the dataframe column datatypes."""
    [check_valid_dtype(d) for d in dtypes.values()]

    df = df.copy()
    for k, v in dtypes.items():
        if k in df.columns:
            df[k] = df[k].astype(v)
    return df

