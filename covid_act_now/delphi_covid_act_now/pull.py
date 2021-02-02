"""Functions for downloading CAN data."""

import pandas as pd

RENAME_COLS = {
    "dt": "timestamp",
    "location": "fips",
}

def load_data(path: str) -> pd.DataFrame:
    """
    Load CAN's data from a local or online parquet file.

    Some important columns are:
    - provider: Source of the data
    - location_type: State or county level data
    - variable_name: Name of available metrics, like pcr_tests_*

    This function also formats and renames the geo and time columns to follow our conventions.

    Parameters
    ----------
    path: str
        A local path or URL to CAN's parquet file to load from

    Returns
    -------
    pd.DataFrame
        CAN's data in long format
    """
    df_pq = (pd
        .read_parquet(path)
        .rename(columns=RENAME_COLS)
    )

    # Format fips
    df_pq["fips"] = df_pq["fips"].astype(str).str.zfill(5)

    return df_pq

def extract_testing_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract just the county-level testing metrics from CAN's data.

    Specifically picks the CDC-sourced metrics only as they are confirmed to be PCR-specimen-based.
    Also converts from long to wide format for easier aggregations later on.

    Note that the CDC's metrics are already smoothed (7-day rolling averaged).

    Parameters
    ----------
    df: pd.DataFrame
        CAN's data in long format

    Returns
    -------
    pd.DataFrame
        CAN's / CDC's testing data in wide format
        Columns: fips, timestamp, pcr_positivity_rate, pcr_tests_positive, pcr_tests_total
    """
    # Filter to PCR-specimen rows from CDC and convert from long to wide format
    df_tests = (
        df
        .query(
            """
            age == 'all' and ethnicity == 'all' and sex == 'all' and \
            location_type == 'county' and provider == 'cdc' and \
            variable_name.str.startswith('pcr_tests_')
            """)
        .pivot(index=["fips", "timestamp"], columns="variable_name", values="value")
        .reset_index()
        # Filter off rows with 0 sample_size
        .query("pcr_tests_total > 0")
        # pcr_tests_positive from the CDC is actually positivity rate (percentage)
        .rename(columns={"pcr_tests_positive": "pcr_positivity_rate"})
    )

    df_tests["pcr_positivity_rate"] /= 100
    df_tests["pcr_tests_positive"] = df_tests.pcr_positivity_rate * df_tests.pcr_tests_total

    return df_tests
