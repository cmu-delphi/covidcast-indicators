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

    def __init__(self):
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
        data = self.gmpr.add_geocode(data,
                                     "fips",
                                     "msa",
                                     from_col=Config.GEO_COL,
                                     new_col="cbsa_id")
        data.drop(columns=Config.GEO_COL, inplace=True)
        data = data.groupby([Config.DATE_COL, "cbsa_id"]).sum().reset_index()

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
                                     from_col=Config.GEO_COL)
        data.drop(columns=Config.GEO_COL, inplace=True)
        data = data.groupby([Config.DATE_COL, "state_id"]).sum().reset_index()

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
        data = self.gmpr.add_geocode(data,
                                     "fips",
                                     "hrr",
                                     from_col=Config.GEO_COL)
        data.drop(columns=Config.GEO_COL, inplace=True)

        ## do a weighted sum by the wpop column to get each HRR's contribution
        tmp = data.groupby([Config.DATE_COL, "hrr"])
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

        dates = np.unique(data[Config.DATE_COL])
        fipss = np.unique(data[Config.GEO_COL])

        # get denominator by day and location for all possible date-fips pairs
        # this fills in 0 if unobserved
        denom_dayloc = np.zeros((len(dates), len(fipss)))
        by_fips = data.groupby(Config.GEO_COL)
        for j, fips in enumerate(fipss):
            denom_dayloc[:, j] = DoctorVisitsSensor.fill_dates(
                by_fips.get_group(fips).set_index(Config.DATE_COL), dates
            )["Denominator"].values

        # get rolling sum across <threshold_len> days
        num_recent_visits = np.concatenate(
            (np.zeros((threshold_len, len(fipss))), np.cumsum(denom_dayloc, axis=0)),
            axis=0,
        )
        num_recent_visits = (
            num_recent_visits[threshold_len:] - num_recent_visits[:-threshold_len]
        )
        recent_visits_df = pd.DataFrame(
            [
                (dates[x[0]], fipss[x[1]], val)
                for x, val in np.ndenumerate(num_recent_visits)
            ],
            columns=[Config.DATE_COL, Config.GEO_COL, "RecentVisits"],
        )
        data = data.merge(
            recent_visits_df, how="left", on=[Config.DATE_COL, Config.GEO_COL]
        )

        all_data = self.gmpr.fips_to_megacounty(data,
                                            threshold_visits,
                                            threshold_len,
                                            fips_col=Config.GEO_COL,
                                            thr_col="Denominator",
                                            date_col=Config.DATE_COL)
        all_data.rename({"megafips": Config.GEO_COL}, axis=1, inplace=True)
        megacounties = all_data[all_data[Config.GEO_COL].str.endswith("000")]
        data = pd.concat([data, megacounties])

        return data.groupby(Config.GEO_COL), Config.GEO_COL
