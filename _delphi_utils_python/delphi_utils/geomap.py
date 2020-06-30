"""Contains geographic mapping tools.

NOTE: This file is mostly duplicated in the Quidel pipeline; bugs fixed here
should be fixed there as well.

Author: James Sharpnack @jsharpna
Based on code by Maria Jahja
Created: 2020-06-01
"""

from os.path import join

import pandas as pd
import numpy as np
from os import path
import pkg_resources

DATA_PATH = "data"
ZIP_FIPS_FILE = "zip_fips_cross_2020.csv"
STATE_FILE = "state_codes.csv"

class GeoMapper:
    """Class to map counties to other geographic resolutions."""

    def __init__(self,
                 fips_filepath=path.join(DATA_PATH,ZIP_FIPS_FILE),
                 state_filepath=path.join(DATA_PATH,STATE_FILE)):
        self.fips_filepath = fips_filepath
        self.state_filepath = state_filepath

    def load_zip_fips_cross(self):
        stream = pkg_resources.resource_stream(__name__, self.fips_filepath)
        self.zip_fips_cross = pd.read_csv(stream,dtype={'zip':str,
                                          'fips':str,
                                          'weight':float})

    def load_state_cross(self):
        stream = pkg_resources.resource_stream(__name__, self.state_filepath)
        self.stcode_cross = pd.read_csv(stream,dtype={'st_code':str,
                                         'state_id':str,
                                         'state_name':str})

    @staticmethod
    def str_convert_fips(x):
        """Ensure fips is a string of length 5."""
        return str(x).zfill(5)

    def convert_fips_to_stcode(self,fips_ser):
        """convert fips string Series to state code string Series"""
        return fips_ser.str[:2]

    def convert_stcode_to_stid(self, stcode_ser):
        """convert fips string Series to state code string Series"""



    # @staticmethod
    # def fill_dates(y_data, dates):
    #     """Ensure all dates are listed in the data, otherwise, add days with 0 counts.
    #
    #     Args:
    #       y_data: dataframe with datetime index
    #       dates: list of datetime to include
    #
    #     Returns:
    #          dataframe containing all dates given
    #     """
    #     first_date = dates[0]
    #     last_date = dates[-1]
    #     cols = y_data.columns
    #
    #     if first_date not in y_data.index:
    #         y_data = y_data.append(pd.DataFrame(dict.fromkeys(cols, 0.),
    #                                             columns=cols, index=[first_date]))
    #     if last_date not in y_data.index:
    #         y_data = y_data.append(pd.DataFrame(dict.fromkeys(cols, 0.),
    #                                             columns=cols, index=[last_date]))
    #
    #     y_data.sort_index(inplace=True)
    #     y_data = y_data.asfreq('D', fill_value=0)
    #     return y_data
    #
    # def county_to_msa(self, data):
    #     """Aggregate county data to the msa resolution.
    #
    #     Args:
    #         data: dataframe aggregated to the daily-county resolution
    #
    #     Returns:
    #         dataframe indexed at the daily-msa resolution
    #
    #     """
    #     msa_map = pd.read_csv(
    #         join(self.geo_filepath, "02_20_uszips.csv"),
    #         usecols=["fips", "cbsa_id"],
    #         dtype={"cbsa_id": float},
    #         converters={"fips": GeoMaps.convert_fips},
    #     )
    #     msa_map.drop_duplicates(inplace=True)
    #     data = data.reset_index()
    #     data = data.merge(msa_map, how="left", left_on=Config.FIPS_COL, right_on="fips")
    #     data = data[~data["cbsa_id"].isna()]
    #     data.drop(columns=["fips", Config.FIPS_COL], inplace=True)
    #     data = data.groupby(["cbsa_id", "date"]).sum()
    #
    #     return data
    #
    # def county_to_state(self, data):
    #     """Aggregate county data to the state resolution.
    #
    #     Args:
    #         data: dataframe aggregated to the daily-county resolution
    #
    #     Returns:
    #         dataframe indexed at the daily-state resolution
    #
    #     """
    #     state_map = pd.read_csv(
    #         join(self.geo_filepath, "02_20_uszips.csv"),
    #         usecols=["fips", "state_id"],
    #         dtype={"state_id": str},
    #         converters={"fips": GeoMaps.convert_fips},
    #     )
    #     state_map.drop_duplicates(inplace=True)
    #     data = data.reset_index()
    #     data = data.merge(
    #         state_map, how="left", left_on=Config.FIPS_COL, right_on="fips"
    #     )
    #     data = data[~data["state_id"].isna()]
    #     data.drop(columns=[Config.FIPS_COL, "fips"], inplace=True)
    #     data = data.groupby(["state_id", "date"]).sum()
    #
    #     return data
    #
    # def hrr(self, data):
    #     """Prepare hrr (Hospital Referral Region) groups.
    #
    #     Args:
    #         data: dataframe aggregated to the daily-hrr resolution
    #
    #     Returns:
    #         dataframe indexed at the daily-hrr resolution
    #
    #     """
    #
    #     return data.groupby("hrr")
    #
    # def county_to_megacounty(self,
    #                          data, threshold_visits=Config.MIN_DEN,
    #                          threshold_len=Config.MAX_BACKFILL_WINDOW):
    #     """Prepare county and megacounty groups. A megacounty for a given day is all of
    #     the counties in a certain state who have a denominator sum over <threshold_len>
    #     days below <threshold_visits>.
    #
    #     Args:
    #         data: dataframe aggregated to the daily-county resolution
    #         threshold_visits: minimum number of total visits needed to create an estimate
    #         threshold_len: maximum number of days to aggregate the total number of visits
    #
    #     Returns:
    #         dataframe at the daily-county resolution, including megacounty rows
    #
    #     """
    #
    #     data = data.reset_index()
    #     dates = np.unique(data["date"])
    #     fipss = np.unique(data["fips"])
    #
    #     # get denominator by day and location for all possible date-fips pairs
    #     # this fills in 0 if unobserved
    #     denom_dayloc = np.zeros((len(dates), len(fipss)))
    #     by_fips = data.groupby("fips")
    #     for j, fips in enumerate(fipss):
    #         denom_dayloc[:, j] = GeoMaps.fill_dates(
    #             by_fips.get_group(fips).set_index("date"), dates
    #         )["den"].values
    #
    #     # get rolling sum across <threshold_len> days
    #     num_recent_visits = np.concatenate(
    #         (np.zeros((threshold_len, len(fipss))), np.cumsum(denom_dayloc, axis=0)),
    #         axis=0,
    #     )
    #     num_recent_visits = (
    #         num_recent_visits[threshold_len:] - num_recent_visits[:-threshold_len]
    #     )
    #     recent_visits_df = pd.DataFrame(
    #         [
    #             (dates[x[0]], fipss[x[1]], val)
    #             for x, val in np.ndenumerate(num_recent_visits)
    #         ],
    #         columns=["date", "fips", "recent_visits"],
    #     )
    #     data = data.merge(
    #         recent_visits_df, how="left", on=["date", "fips"]
    #     )
    #
    #     # mark date-fips points to exclude if we see less than threshold visits that day
    #     data["to_exclude"] = data["recent_visits"] < threshold_visits
    #
    #     # now to convert to megacounties
    #     state_map = pd.read_csv(
    #         join(self.geo_filepath, "02_20_uszips.csv"),
    #         usecols=["fips", "state_id"],
    #         dtype={"state_id": str},
    #         converters={"fips": GeoMaps.convert_fips},
    #     )
    #     state_map.drop_duplicates(inplace=True)
    #     data = data.merge(
    #         state_map, how="left", left_on="fips", right_on="fips"
    #     )
    #     # drops rows with no matches, which should not be many
    #     data.dropna(inplace=True)
    #     data["state_fips"] = data["fips"].str[:2] + '000'
    #
    #     megacounty_df = (
    #         data[data["to_exclude"]]
    #             .groupby(["date", "state_fips"])
    #             .sum()
    #             .reset_index()
    #     )
    #     megacounty_df["to_exclude"] = False
    #     megacounty_df.rename(columns={"state_fips": "fips"}, inplace=True)
    #
    #     result = pd.concat([data, megacounty_df])
    #     result.drop(
    #         columns=["state_fips", "state_id", "to_exclude", "recent_visits"],
    #         inplace=True
    #     )
    #     result = result.groupby(["fips", "date"]).sum()
    #
    #     return result
