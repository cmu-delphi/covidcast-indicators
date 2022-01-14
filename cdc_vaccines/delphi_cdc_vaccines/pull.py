# -*- coding: utf-8 -*-
"""Functions for pulling data from the CDC data website for vaccines."""
import hashlib
from logging import Logger
from delphi_utils.geomap import GeoMapper
import numpy as np
import pandas as pd
from .constants import SIGNALS, DIFFERENCE_MAPPING



def pull_cdcvacc_data(base_url: str, export_start_date: str,
    export_end_date: str, logger: Logger) -> pd.DataFrame:
    """Pull the latest data from the CDC on vaccines and conform it into a dataset.

    The output dataset has:
    - Each row corresponds to (County, Date), denoted (FIPS, timestamp)
    - Each row additionally has columns that correspond to the counts or
      cumulative counts of vaccination status (fully vaccinated,
      partially vaccinated) of various age groups (all, 12+, 18+, 65+)
      from December 13th 2020 until the latest date

    Note that the raw dataset gives the `cumulative` metrics, from which
    we compute `counts` by taking first differences.  Hence, `counts`
    may be negative.  This is wholly dependent on the quality of the raw
    dataset.

    We filter the data such that we only keep rows with valid FIPS, or "FIPS"
    codes defined under the exceptions of the README.  The current  exceptions
    include:
    # - 0: statewise unallocated
    Parameters
    ----------
    base_url: str
        Base URL for pulling the CDC Vaccination Data
    export_start_date: str
        The start date for the csv file (can be empty)
    export_end_date:
        The end date for the csv file (can be empty)
    logger: Logger
    Returns
    -------
    pd.DataFrame
        Dataframe as described above.
    """
    # Columns to drop the the data frame.
    drop_columns = [
    "date",
    "recip_state",
    "series_complete_pop_pct",
    "mmwr_week",
    "recip_county",
    "state_id"
    ]

    # Read data and cut off by designated start date
    df = pd.read_csv(base_url)
    df['Date']=pd.to_datetime(df['Date'])
    try:
        export_start_date = pd.to_datetime(0) if (pd.to_datetime(export_start_date)
            is pd.NaT) else pd.to_datetime(export_start_date)
        export_end_date = pd.Timestamp.max if (pd.to_datetime(export_end_date)
            is pd.NaT) else pd.to_datetime(export_end_date)
    except KeyError as e:
        raise ValueError(
            "Tried to convert export_start/end_date param "
            "to datetime but failed. Please "
            "check this input."
        ) from e
    try:
        df = df.query('@export_start_date <= Date')
        df = df.query('Date <= @export_end_date')
    except KeyError as e:
        raise ValueError(
            "Used export_start/end_date param "
            "to filter dataframe but failed. Please "
            "check this input."
        ) from e
    if df['Date'].shape[0] == 0:
        raise ValueError(
            "Output df has no rows. Please check  "
            "if export_start_date is later than "
            "export_end_date. Else check if base_url"
            " still functional."
        )

    logger.info("data retrieved from source",
                num_rows=df.shape[0],
                num_cols=df.shape[1],
                min_date=min(df['Date']),
                max_date=max(df['Date']),
                checksum=hashlib.sha256(pd.util.hash_pandas_object(df).values).hexdigest())
    df.columns = [i.lower() for i in df.columns]

    df.loc[:,'recip_state'] = df['recip_state'].str.lower().copy()

    drop_columns =  list(set(drop_columns + [x for x in df.columns if
        ("pct" in x) | ("svi" in x)] + list(df.columns[22:])))
    df = GeoMapper().add_geocode(df, "state_id", "state_code",
        from_col="recip_state", new_col="state_id", dropna=False)
    df['state_id'] = df['state_id'].fillna('0').astype(int)
    # Change FIPS from 0 to XX000 for statewise unallocated cases/deaths
    unassigned_index = (df["fips"] == "UNK")
    df.loc[unassigned_index, "fips"] = df["state_id"].loc[unassigned_index].values * 1000

    # Conform FIPS
    df["fips"] = df["fips"].apply(lambda x: f"{int(x):05d}")
    df["timestamp"] = pd.to_datetime(df["date"])
    # Drop unnecessary columns (state is pre-encoded in fips)
    try:
        df.drop(drop_columns, axis=1, inplace=True)
    except KeyError as e:
        raise ValueError(
            "Tried to drop non-existent columns. The dataset "
            "schema may have changed.  Please investigate and "
            "amend drop_columns."
        ) from e

    # timestamp: str -> datetime
    try:
        df.columns = ["fips",
                      "cumulative_counts_tot_vaccine",
                      "cumulative_counts_tot_vaccine_12P",
                      "cumulative_counts_tot_vaccine_18P",
                      "cumulative_counts_tot_vaccine_65P",
                      "cumulative_counts_part_vaccine",
                      "cumulative_counts_part_vaccine_12P",
                      "cumulative_counts_part_vaccine_18P",
                      "cumulative_counts_part_vaccine_65P",
                      "timestamp"]
    except KeyError as e:
        raise ValueError(
            "Tried to name wrong number of columns. The dataset "
            "schema may have changed.  Please investigate and "
            "amend drop_columns."
        ) from e

    df_dummy = df.loc[(df["timestamp"] == min(df["timestamp"]))].copy()
    df_dummy.loc[:, "timestamp"] = df_dummy.loc[:, "timestamp"] - pd.Timedelta(days=1)
    df_dummy.loc[:, ["cumulative_counts_tot_vaccine",
                    "cumulative_counts_tot_vaccine_12P",
                    "cumulative_counts_tot_vaccine_18P",
                    "cumulative_counts_tot_vaccine_65P",
                    "cumulative_counts_part_vaccine",
                    "cumulative_counts_part_vaccine_12P",
                    "cumulative_counts_part_vaccine_18P",
                    "cumulative_counts_part_vaccine_65P",
                    ]] = 0

    df =pd.concat([df_dummy, df])
    df = df.set_index(["fips", "timestamp"])
    for to, from_d in DIFFERENCE_MAPPING.items():
        df[to] = df.groupby(level=0)[from_d].diff()
   df.reset_index(inplace=True)
    # Final sanity checks
    unique_days = df["timestamp"].unique()
    n_days = (max(unique_days) - min(unique_days)) / np.timedelta64(1, "D") + 1
    if n_days != len(unique_days):
        raise ValueError(
            f"Not every day between {min(unique_days)} and "
            "{max(unique_days)} is represented."
        )
    return df.loc[
        df["timestamp"] >= min(df["timestamp"]),
        # Reorder
        ["fips", "timestamp"] + SIGNALS,
    ].reset_index(drop=True)
