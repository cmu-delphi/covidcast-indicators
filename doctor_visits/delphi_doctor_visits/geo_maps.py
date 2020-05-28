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

from .config import Config
from .sensor import DoctorVisitsSensor


class GeoMaps:
    """Class to map counties to other geographic resolutions."""

    def __init__(self, geo_filepath):
        self.geo_filepath = geo_filepath

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
        data = data.merge(msa_map, how="left", left_on="PatCountyFIPS", right_on="fips")
        data.dropna(inplace=True)
        data.drop(columns=["fips", "PatCountyFIPS"], inplace=True)
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
        data = data.merge(
            state_map, how="left", left_on="PatCountyFIPS", right_on="fips"
        )
        data.dropna(inplace=True)
        data.drop(columns=["PatCountyFIPS", "fips"], inplace=True)
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

        data = data.merge(hrr_map, how="left", left_on="PatCountyFIPS", right_on="fips")
        ## drops rows with no matching HRR, which should not be many
        data.dropna(inplace=True)
        data.drop(columns=["PatCountyFIPS", "fips"], inplace=True)

        ## do a weighted sum by the wpop column to get each HRR's contribution
        tmp = data.groupby(["ServiceDate", "hrr"])
        wtsum = lambda g: g["wpop"].values @ g[Config.COUNT_COLS]
        data = tmp.apply(wtsum).reset_index()

        return data.groupby("hrr"), "hrr"

    def county_to_megacounty(self, data, threshold_visits, threshold_len):
        """A megacounty for a given day is all of the counties in a certain state who have:
                 1) Denominator sum over <threshold_len> days below <threshold_visits>, or
                 2) 0 denominator the last <min_recent_obs> days (not relevant for code
                    because 0 denominator is not present)

        Args:
            data: dataframe aggregated to the daily-county resolution (all 7 cols expected)

        Returns: tuple of dataframe at the daily-state resolution, and geo_id column name
        """

        dates = np.unique(data["ServiceDate"])
        fipss = np.unique(data["PatCountyFIPS"])

        # get denominator by day and location for all possible date-fips pairs
        # this fills in 0 if unobserved
        denom_dayloc = np.zeros((len(dates), len(fipss)))
        by_fips = data.groupby("PatCountyFIPS")
        for j, fips in enumerate(fipss):
            denom_dayloc[:, j] = DoctorVisitsSensor.fill_dates(
                by_fips.get_group(fips).set_index("ServiceDate"), dates
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
            columns=["ServiceDate", "PatCountyFIPS", "RecentVisits"],
        )
        data = data.merge(
            recent_visits_df, how="left", on=["ServiceDate", "PatCountyFIPS"]
        )

        # mark date-fips points to exclude if we see less than threshold visits that day
        data["ToExclude"] = data["RecentVisits"] < threshold_visits

        # now to convert to megacounties
        state_map = pd.read_csv(
            join(self.geo_filepath, "02_20_uszips.csv"),
            usecols=["fips", "state_id"],
            dtype={"state_id": str},
            converters={"fips": GeoMaps.convert_fips},
        )
        state_map.drop_duplicates(inplace=True)
        data = data.merge(
            state_map, how="left", left_on="PatCountyFIPS", right_on="fips"
        )
        # drops rows with no matches, which should not be many
        data.dropna(inplace=True)
        data.drop(columns=["fips"], inplace=True)
        data["StateFIPS"] = data["PatCountyFIPS"].apply(
            lambda f: str((int(f) // 1000) * 1000).zfill(5)
        )

        megacounty_df = (
            data[data["ToExclude"]]
            .groupby(["ServiceDate", "StateFIPS"])
            .sum()
            .reset_index()
        )
        megacounty_df["ToExclude"] = False
        megacounty_df.rename(columns={"StateFIPS": "PatCountyFIPS"}, inplace=True)

        result = pd.concat([data, megacounty_df])
        result.drop(columns=["StateFIPS", "state_id"], inplace=True)

        return result.groupby("PatCountyFIPS"), "PatCountyFIPS"
