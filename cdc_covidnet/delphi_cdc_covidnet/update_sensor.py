"""
Generate COVID-NET sensors.

Author: Eu Jing Chua
Created: 2020-06-12
"""

from datetime import datetime
from os.path import join
from typing import List

import numpy as np
import pandas as pd

from delphi_utils import read_params, GeoMapper, add_prefix
from .api_config import APIConfig
from .covidnet import CovidNet
from .constants import SIGNALS

def write_to_csv(data: pd.DataFrame, out_name: str, output_path: str):
    """
    Write sensor values to csv.

    The dataframe be indexed by (date, geo_id), with columns
    values, se, sample_size

    Args:
        data: pd.DataFrame to write to csvs
        output_name: Suffix name to add to each output file
        output_path: Path to write the csvs to
    """
    # Each date is a csv file
    dates = data.index.get_level_values("date").unique()
    for date in dates:
        # Each csv file is indexed by geo_id
        sub_df = data.loc[date, :]

        # There should only be one epiweek number for this week
        assert len(sub_df["epiweek"].unique()) == 1
        epiweek = int(sub_df["epiweek"].unique()[0])

        filename = join(output_path, "{}{:02}_state_{}.csv".format(
            date.strftime("%Y"), epiweek, out_name))

        # Drop extra epiweek column before writing to csv
        sub_df.drop("epiweek", axis=1).to_csv(filename, na_rep="NA")


def update_sensor(
        state_files: List[str],
        mmwr_info: pd.DataFrame,
        output_path: str,
        start_date: datetime,
        end_date: datetime) -> pd.DataFrame:
    """
    Generate sensor values, and write to csv format.

    Args:
        state_files: List of JSON files representing COVID-NET hospitalization data for each state
        mmwr_info: Mappings from MMWR week to actual dates, as a pd.DataFrame
        output_path: Path to write the csvs to
        start_date: First sensor date (datetime.datetime)
        end_date: Last sensor date (datetime.datetime)

    Returns:
        The overall pd.DataFrame after all processing
    """
    assert start_date < end_date, "start_date >= end_date"

    # Combine and format hospitalizations dataframe
    hosp_df = CovidNet.read_all_hosp_data(state_files)
    hosp_df = hosp_df.merge(mmwr_info, how="left",
                            left_on=["mmwr-year", "mmwr-week"],
                            right_on=["year", "weeknumber"])

    # Select relevant columns and standardize naming
    hosp_df = hosp_df.loc[:, APIConfig.HOSP_RENAME_COLS.keys()]\
        .rename(columns=APIConfig.HOSP_RENAME_COLS)

    # Restrict to start and end date
    hosp_df = hosp_df[
        (hosp_df["date"] >= start_date) & (
            hosp_df["date"] < end_date)
    ]

    # Set state id to two-letter abbreviation
    gmpr = GeoMapper()
    hosp_df = gmpr.add_geocode(hosp_df,
                               from_col=APIConfig.STATE_COL,
                               from_code="state_name",
                               new_code="state_id",
                               dropna=False)
    # To use the original column name, reassign original column and drop new one
    hosp_df[APIConfig.STATE_COL] = hosp_df["state_id"]
    hosp_df.drop("state_id", axis=1, inplace=True)
    assert not hosp_df.duplicated(["date", "geo_id"]).any(), "Non-unique (date, geo_id) pairs"
    hosp_df.set_index(["date", "geo_id"], inplace=True)

    # Fill in remaining expected columns
    hosp_df["se"] = np.nan
    hosp_df["sample_size"] = np.nan

    # Write results
    signals = add_prefix(SIGNALS, wip_signal=read_params()["wip_signal"], prefix="wip_")
    for signal in signals:
        write_to_csv(hosp_df, signal, output_path)
    return hosp_df
