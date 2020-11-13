"""Contains geographic mapping tools.

NOTE: This file is mostly duplicated in the Quidel pipeline; bugs fixed here
should be fixed there as well.

Author: Maria Jahja
Created: 2020-04-18
Last modified: 2020-04-30 by Aaron Rumack (add megacounty code)
"""

from os.path import join

import pandas as pd
import numpy as np
from delphi_utils.geomap import GeoMapper

from .config import Config
from .sensor import DoctorVisitsSensor


class GeoMaps:
    """Class to map counties to other geographic resolutions."""

    def __init__(self, geo_filepath):
        self.geo_filepath = geo_filepath
        self.gmpr = GeoMapper()

    @staticmethod
    def convert_fips(x):
        """Ensure fips is a string of length 5."""
        return str(x).zfill(5)

    def county_to_msa(self, data):
        """Aggregate county data to the msa resolution.

        Args:
            data: dataframe aggregated to the daily-county resolution (all 7 cols expected)

        Returns: tuple of dataframe at the daily-msa resolution, and the geo_id column name
        """
        msa_map = pd.read_csv(
            join(self.geo_filepath, "02_20_uszips.csv"),
            usecols=["fips", "cbsa_id"],
            dtype={"cbsa_id": float},
            converters={"fips": GeoMaps.convert_fips},
        )
        msa_map.drop_duplicates(inplace=True)
        data = self.gmpr.add_geocode(data,
                                     "fips",
                                     "msa",
                                     from_col="PatCountyFIPS",
                                     new_col="cbsa_id")
        data.drop(columns="PatCountyFIPS", inplace=True)
        data = data.groupby(["ServiceDate", "cbsa_id"]).sum().reset_index()

        return data.groupby("cbsa_id"), "cbsa_id"

    def county_to_state(self, data):
        """Aggregate county data to the state resolution.

        Args:
            data: dataframe aggregated to the daily-county resolution (all 7 cols expected)

        Returns: tuple of dataframe at the daily-state resolution, and geo_id column name
        """

        state_map = pd.read_csv(
            join(self.geo_filepath, "02_20_uszips.csv"),
            usecols=["fips", "state_id"],
            dtype={"state_id": str},
            converters={"fips": GeoMaps.convert_fips},
        )
        state_map.drop_duplicates(inplace=True)
        data = self.gmpr.add_geocode(data,
                                     "fips",
                                     "state_id",
                                     from_col="PatCountyFIPS")
        data.drop(columns="PatCountyFIPS", inplace=True)
        data = data.groupby(["ServiceDate", "state_id"]).sum().reset_index()

        return data.groupby("state_id"), "state_id"

    def county_to_hrr(self, data):
        """Aggregate county data to the HRR resolution.

        Note that counties are not strictly contained within HRRs. When a county
        spans boundaries, we report it with the same rate in each containing HRR,
        but with a sample size weighted by how much it overlaps that HRR.

        Args:
            data: dataframe aggregated to the daily-county resolution (all 7 cols expected)

        Returns:
            tuple of (data frame at daily-HRR resolution, geo_id column name)

        """

        hrr_map = pd.read_csv(
            join(self.geo_filepath, "transfipsToHRR.csv"),
            converters={"fips": GeoMaps.convert_fips},
        )

        ## Each row is one FIPS. Columns [3:] are HRR numbers, consecutively.
        ## Entries are the proportion of the county contained in the HRR, so rows
        ## sum to 1.

        ## Drop county and state names -- not needed here.
        hrr_map.drop(columns=["county_name", "state_id"], inplace=True)

        hrr_map = hrr_map.melt(["fips"], var_name="hrr", value_name="wpop")
        hrr_map = hrr_map[hrr_map["wpop"] > 0]
        data = self.gmpr.add_geocode(data,
                                     "fips",
                                     "hrr",
                                     from_col="PatCountyFIPS")
        data.drop(columns="PatCountyFIPS", inplace=True)

        ## do a weighted sum by the wpop column to get each HRR's contribution
        tmp = data.groupby(["ServiceDate", "hrr"])
        wtsum = lambda g: g["weight"].values @ g[Config.COUNT_COLS]
        data = tmp.apply(wtsum).reset_index()

        return data.groupby("hrr"), "hrr"

    def county_to_megacounty(self, data, threshold_visits, threshold_len):
        """Convert to megacounty and groupby FIPS using GeoMapper package.

        Args:
            data: dataframe aggregated to the daily-county resolution (all 7 cols expected)
            threshold_visits: count threshold to determine when to convert to megacounty.
            threshold_len: number of days to use when thresholding.

        Returns: tuple of dataframe at the daily-state resolution, and geo_id column name
        """
        data = self.gmpr.fips_to_megacounty(data,
                                            threshold_visits,
                                            threshold_len,
                                            fips_col="PatCountyFIPS",
                                            thr_col="Denominator",
                                            date_col="ServiceDate")
        data.rename({"megafips": "PatCountyFIPS"}, axis=1, inplace=True)
        return data.groupby("PatCountyFIPS"), "PatCountyFIPS"
