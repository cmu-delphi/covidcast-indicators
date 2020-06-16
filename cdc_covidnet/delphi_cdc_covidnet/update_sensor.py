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

from .api_config import APIConfig
from .covidnet import CovidNet
from .geo_maps import GeoMaps

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
        state_files: List[str], mmwr_info: pd.DataFrame,
        output_path: str, static_path: str,
        start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Generate sensor values, and write to csv format.

    Args:
        state_files: List of JSON files representing COVID-NET hospitalization data for each state
        mmwr_info: Mappings from MMWR week to actual dates, as a pd.DataFrame
        output_path: Path to write the csvs to
        static_path: Path for the static geographic fiels
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
    geo_map = GeoMaps(static_path)
    hosp_df = geo_map.state_name_to_abbr(hosp_df)

    assert not hosp_df.duplicated(["date", "geo_id"]).any(), "Non-unique (date, geo_id) pairs"
    hosp_df.set_index(["date", "geo_id"], inplace=True)

    # Fill in remaining expected columns
    hosp_df["se"] = np.nan
    hosp_df["sample_size"] = np.nan

    # Write results
    out_name = "wip_covidnet"
    write_to_csv(hosp_df, out_name, output_path)

    return hosp_df
