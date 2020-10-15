"""Contains geographic mapping tools.

NOTE: This file is mostly duplicated in the Quidel pipeline; bugs fixed here
should be fixed there as well.

Author: Aaron Rumack
Created: 2020-10-14
"""

from os.path import join

import pandas as pd
import numpy as np

from .config import Config


class GeoMaps:
    """Class to map counties to other geographic resolutions."""

    def __init__(self, geo_filepath):
        self.geo_filepath = geo_filepath

    @staticmethod
    def convert_fips(x):
        """Ensure fips is a string of length 5."""
        return str(x).zfill(5)

    @staticmethod
    def fill_dates(y_data, dates):
        """Ensure all dates are listed in the data, otherwise, add days with 0 counts.

        Args:
          y_data: dataframe with datetime index
          dates: list of datetime to include

        Returns:
             dataframe containing all dates given
        """
        first_date = dates[0]
        last_date = dates[-1]
        cols = y_data.columns

        if first_date not in y_data.index:
            y_data = y_data.append(pd.DataFrame(dict.fromkeys(cols, 0.),
                                                columns=cols, index=[first_date]))
        if last_date not in y_data.index:
            y_data = y_data.append(pd.DataFrame(dict.fromkeys(cols, 0.),
                                                columns=cols, index=[last_date]))

        y_data.sort_index(inplace=True)
        y_data = y_data.asfreq('D', fill_value=0)
        return y_data

    def county_to_msa(self, data):
        """Aggregate county data to the msa resolution.

        Args:
            data: dataframe aggregated to the daily-county resolution

        Returns:
            dataframe indexed at the daily-msa resolution

        """
        msa_map = pd.read_csv(
            join(self.geo_filepath, "02_20_uszips.csv"),
            usecols=["fips", "cbsa_id"],
            dtype={"cbsa_id": float},
            converters={"fips": GeoMaps.convert_fips},
        )
        msa_map.drop_duplicates(inplace=True)
        data = data.reset_index()
        data = data.merge(msa_map, how="left", left_on=Config.FIPS_COL, right_on="fips")
        data = data[~data["cbsa_id"].isna()]
        data.drop(columns=["fips", Config.FIPS_COL], inplace=True)
        data = data.groupby(["cbsa_id", Config.DATE_COL]).sum()

        return data

    def county_to_state(self, data):
        """Aggregate county data to the state resolution.

        Args:
            data: dataframe aggregated to the daily-county resolution

        Returns:
            dataframe indexed at the daily-state resolution

        """
        state_map = pd.read_csv(
            join(self.geo_filepath, "02_20_uszips.csv"),
            usecols=["fips", "state_id"],
            dtype={"state_id": str},
            converters={"fips": GeoMaps.convert_fips},
        )
        state_map.drop_duplicates(inplace=True)
        data = data.reset_index()
        data = data.merge(
            state_map, how="left", left_on=Config.FIPS_COL, right_on="fips"
        )
        data = data[~data["state_id"].isna()]
        data.drop(columns=[Config.FIPS_COL, "fips"], inplace=True)
        data = data.groupby(["state_id", Config.DATE_COL]).sum()

        return data

    def county_to_hrr(self, data):
        """Prepare hrr (Hospital Referral Region) groups.

        Args:
            data: dataframe aggregated to the daily-hrr resolution

        Returns:
            dataframe indexed at the daily-hrr resolution

        """

        hrr_map = pd.read_csv(self.geo_filepath / "transfipsToHRR.csv",
                              converters={"fips": GeoMaps.convert_fips})

        ## Each row is one FIPS. Columns [3:] are HRR numbers, consecutively.
        ## Entries are the proportion of the county contained in the HRR, so rows
        ## sum to 1.

        ## Drop county and state names -- not needed here.
        hrr_map.drop(columns=["county_name", "state_id"], inplace=True)

        hrr_map = hrr_map.melt(["fips"], var_name="hrr", value_name="wpop")
        hrr_map = hrr_map[hrr_map["wpop"] > 0]

        hrr_map.drop_duplicates(inplace=True)
        data = data.reset_index()

        data = data.merge(hrr_map, how="left", left_on=Config.GEO_COL, right_on="fips")
        ## drops rows with no matching HRR, which should not be many
        data = data[~data["state_id"].isna()]
        data.drop(columns=[Config.GEO_COL, "fips"], inplace=True)

        ## do a weighted sum by the wpop column to get each HRR's contribution
        tmp = data.groupby([Config.DATE_COL, "hrr"])
        wtsum = lambda g: g["wpop"].values @ g[Config.COUNT_COLS]
        data = tmp.apply(wtsum).reset_index()

        data = data.groupby(["hrr", Config.DATE_COL]).sum()

        return data

    def county_to_megacounty(self,
                             data, threshold_visits=Config.MIN_DEN,
                             threshold_len=Config.MAX_BACKFILL_WINDOW):
        """Prepare county and megacounty groups. A megacounty for a given day is all of
        the counties in a certain state who have a denominator sum over <threshold_len>
        days below <threshold_visits>.

        Args:
            data: dataframe aggregated to the daily-county resolution
            threshold_visits: minimum number of total visits needed to create an estimate
            threshold_len: maximum number of days to aggregate the total number of visits

        Returns:
            dataframe at the daily-county resolution, including megacounty rows

        """

        data = data.reset_index()
        dates = np.unique(data[Config.DATE_COL])
        fipss = np.unique(data[Config.GEO_COL])

        # get denominator by day and location for all possible date-fips pairs
        # this fills in 0 if unobserved
        denom_dayloc = np.zeros((len(dates), len(fipss)))
        by_fips = data.groupby(Config.GEO_COL)
        for j, fips in enumerate(fipss):
            denom_dayloc[:, j] = GeoMaps.fill_dates(
                by_fips.get_group(fips).set_index(Config.DATE_COL), dates
            )[Config.DENOMINATOR].values

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
            columns=[Config.DATE_COL, Config.GEO_COL, "recent_visits"],
        )
        data = data.merge(
            recent_visits_df, how="left", on=[Config.DATE_COL, Config.GEO_COL]
        )

        # mark date-fips points to exclude if we see less than threshold visits that day
        data["to_exclude"] = data["recent_visits"] < threshold_visits

        # now to convert to megacounties
        state_map = pd.read_csv(
            join(self.geo_filepath, "02_20_uszips.csv"),
            usecols=["fips", "state_id"],
            dtype={"state_id": str},
            converters={"fips": GeoMaps.convert_fips},
        )
        state_map.drop_duplicates(inplace=True)
        data = data.merge(
            state_map, how="left", left_on=Config.GEO_COL, right_on="fips"
        )
        # drops rows with no matches, which should not be many
        data.dropna(inplace=True)
        data["state_fips"] = data["fips"].str[:2] + '000'

        megacounty_df = (
            data[data["to_exclude"]]
                .groupby([Config.DATE_COL, "state_fips"])
                .sum()
                .reset_index()
        )
        megacounty_df["to_exclude"] = False
        megacounty_df.rename(columns={"state_fips": Config.GEO_COL}, inplace=True)

        result = pd.concat([data, megacounty_df])
        result.drop(
            columns=["state_fips", "state_id", "to_exclude", "recent_visits"],
            inplace=True
        )
        result = result.groupby([Config.GEO_COL, Config.DATE_COL]).sum()

        return result
