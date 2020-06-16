"""
Contains geographic mapping tools.

Author: Eu Jing Chua
Created: 2020-06-12
"""

from os.path import join

import pandas as pd

from .api_config import APIConfig

class GeoMaps:
    """
    Class to handle any geography-related mappings
    """

    def __init__(self, geo_filepath: str):
        self.geo_filepath = geo_filepath

    def state_name_to_abbr(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Converts the geo_id column from a full state name to the two-letter abbreviation

        Args:
            data: The pd.DataFrame with the geo_id column to be converted

        Returns:
            The modified pd.DataFrame after conversion
        """
        # Read in geographical mappings
        state_map = pd.read_csv(
            join(self.geo_filepath, "02_20_uszips.csv"),
            usecols=["state_id", "state_name"])
        state_map.drop_duplicates(inplace=True)

        # State map is just the Series of state name -> state id
        state_map.set_index("state_name", drop=True, inplace=True)
        state_map = state_map["state_id"]

        # Map state name to state two-letter abbreviation
        data[APIConfig.STATE_COL] = data[APIConfig.STATE_COL].map(state_map)

        return data
