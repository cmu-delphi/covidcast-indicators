"""Contains geographic mapping tools.

NOTE: This file is mostly duplicated in the Quidel pipeline; bugs fixed here
should be fixed there as well.

Author: Maria Jahja
Created: 2020-04-18
Last modified: 2020-04-30 by Aaron Rumack (add megacounty code)
"""
from functools import partial

import pandas as pd
from delphi_utils.geomap import GeoMapper

from .config import Config


class GeoMaps:
    """Class to map counties to other geographic resolutions."""

    def __init__(self):
        """Create the underlying GeoMapper."""
        self.gmpr = GeoMapper()
        self.geo_func = {"county": partial(self.county_to_megacounty,
                                           threshold_visits=Config.MIN_RECENT_VISITS,
                                           threshold_len=Config.RECENT_LENGTH),
                         "state": self.county_to_state,
                         "msa": self.county_to_msa,
                         "hrr": self.county_to_hrr,
                         "hhs": self.county_to_hhs,
                         "nation": self.county_to_nation}

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
        data = self.gmpr.add_geocode(data,
                                     "fips",
                                     "msa",
                                     from_col="PatCountyFIPS",
                                     new_col="cbsa_id")
        data.drop(columns="PatCountyFIPS", inplace=True)
        data = data.groupby(["ServiceDate", "cbsa_id"]).sum(numeric_only=True).reset_index()

        return data.groupby("cbsa_id"), "cbsa_id"

    def county_to_state(self, data):
        """Aggregate county data to the state resolution.

        Args:
            data: dataframe aggregated to the daily-county resolution (all 7 cols expected)

        Returns: tuple of dataframe at the daily-state resolution, and geo_id column name
        """
        data = self.gmpr.add_geocode(data,
                                     "fips",
                                     "state_id",
                                     from_col="PatCountyFIPS")
        data.drop(columns="PatCountyFIPS", inplace=True)
        data = data.groupby(["ServiceDate", "state_id"]).sum(numeric_only=True).reset_index()

        return data.groupby("state_id"), "state_id"

    def county_to_hhs(self, data):
        """Aggregate county data to the HHS region resolution.

        Args:
            data: dataframe aggregated to the daily-county resolution (all 7 cols expected)

        Returns: tuple of dataframe at the daily-HHS resolution, and geo_id column name
        """
        data = self.gmpr.add_geocode(data,
                                     "fips",
                                     "hhs",
                                     from_col="PatCountyFIPS")
        data.drop(columns="PatCountyFIPS", inplace=True)
        data = data.groupby(["ServiceDate", "hhs"]).sum(numeric_only=True).reset_index()

        return data.groupby("hhs"), "hhs"

    def county_to_nation(self, data):
        """Aggregate county data to the nation resolution.

        Args:
            data: dataframe aggregated to the daily-county resolution (all 7 cols expected)

        Returns: tuple of dataframe at the daily-nation resolution, and geo_id column name
        """
        data = self.gmpr.add_geocode(data,
                                     "fips",
                                     "nation",
                                     from_col="PatCountyFIPS")
        data.drop(columns="PatCountyFIPS", inplace=True)
        data = data.groupby(["ServiceDate", "nation"]).sum(numeric_only=True).reset_index()

        return data.groupby("nation"), "nation"

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
        all_data = self.gmpr.fips_to_megacounty(data,
                                            threshold_visits,
                                            threshold_len,
                                            fips_col="PatCountyFIPS",
                                            thr_col="Denominator",
                                            date_col="ServiceDate")
        all_data.rename({"megafips": "PatCountyFIPS"}, axis=1, inplace=True)
        megacounties = all_data[all_data.PatCountyFIPS.str.endswith("000")]
        data = pd.concat([data, megacounties])

        return data.groupby("PatCountyFIPS"), "PatCountyFIPS"
