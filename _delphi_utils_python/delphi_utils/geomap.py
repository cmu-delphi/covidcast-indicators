"""Contains geographic mapping tools.

NOTE: This file is mostly duplicated in the Quidel pipeline; bugs fixed here
should be fixed there as well.

Author: James Sharpnack @jsharpna
Partially based on code by Maria Jahja
Created: 2020-06-01

TODO:
1. increase coverage to include full flags
2. test that the cross files are unchanged
3. add remaining mappings
4. reset_index?
5.
"""

from os.path import join

import pandas as pd
import numpy as np
from os import path
import pkg_resources

DATA_PATH = "data"
ZIP_FIPS_FILE = "zip_fips_cross_2020.csv"
STATE_FILE = "state_codes.csv"
MSA_FILE = "fips_msa_cross_2020.csv"

class GeoMapper:
    """Class for geo mapping tools commonly used in Delphi

    GeoMapper instance will load "crosswalk" data from the package data_dir when needed
    the cross tables convert from one geo to another and then the main defs of the form
    *_* will use then to convert from one resolution to another

    defs can be categorized:
    - load_* : load the cross file which has from and to geo index (such as zip to fips)
      if the mapping is probabilistic then a weight column exists, e.g.
      zip, fips, weight satisfies (sum(weights) where zip==ZIP) == 1
    - convert_* : add new column by joining with cross table
    - *_to_* : replace one geo column with another via weighted sum aggregation
      e.g. (sum(weights*count_column) groupby fips) would convert zip level data
      to fips level data if the zip_fips_cross table is used

     Mappings: (- incomplete)
       - zip -> county : population weighted
       + county -> state : unweighted
       + county -> msa : unweighted
       - county -> megacounty

    """

    def __init__(self,
                 fips_filepath=path.join(DATA_PATH,ZIP_FIPS_FILE),
                 state_filepath=path.join(DATA_PATH,STATE_FILE),
                 msa_filepath=path.join(DATA_PATH,MSA_FILE)):
        """Initialize geomapper

        Args:
            fips_filepath: location of zip->fips cross table
            state_filepath: location of state_code->state_id,name cross table
            msa_filepath: location of fips->msa cross table
        """
        self.fips_filepath = fips_filepath
        self.state_filepath = state_filepath
        self.msa_filepath = msa_filepath

    def load_zip_fips_cross(self):
        """load zip->fips cross table"""
        stream = pkg_resources.resource_stream(__name__, self.fips_filepath)
        self.zip_fips_cross = pd.read_csv(stream,dtype={'zip':str,
                                          'fips':str,
                                          'weight':float})

    def load_state_cross(self):
        """load state_code->state_id cross table"""
        stream = pkg_resources.resource_stream(__name__, self.state_filepath)
        self.stcode_cross = pd.read_csv(stream,dtype={'st_code':str,
                                         'state_id':str,
                                         'state_name':str})

    def load_fips_msa_cross(self):
        """load fips->msa cross table"""
        stream = pkg_resources.resource_stream(__name__, self.msa_filepath)
        self.fips_msa_cross = pd.read_csv(stream, dtype={'fips': str,
                                                       'msa': str})

    def convert_fips_to_stcode(self,
                               data,
                               fips_col='fips',
                               stcode_col='st_code'):
        """create st_code column from fips column

        Args:
            data: pd.DataFrame input data
            fips_col: fips column to convert
            stcode_col: stcode column to create

        Return:
            data: copy of dataframe
        """
        data = data.copy()
        if data[fips_col].dtype != 'O':
            data = self.convert_intfips_to_str(data,intfips_col=fips_col,strfips_col=fips_col)
        data[stcode_col] = data[fips_col].str[:2]
        return data

    @staticmethod
    def convert_int_to_str5(data,
                            int_col='fips',
                            str_col='fips'):
        """convert int to a string of length 5"""
        data = data.copy()
        data[str_col] = data[int_col].astype(str).str.zfill(5)
        return data

    def convert_intfips_to_str(self,
                               data,
                               intfips_col='fips',
                               strfips_col='fips'):
        """convert fips to a string of length 5"""
        return GeoMapper.convert_int_to_str5(data,int_col=intfips_col,str_col=strfips_col)


    def convert_stcode_to_state_id(self,
                               data,
                               stcode_col='st_code',
                               state_id_col='state_id',
                               full=False):
        """create state_id column from st_code column

        Args:
            data: pd.DataFrame input data
            stcode_col: stcode column to convert
            state_id_col: state_id column to create
            full: boolean, if True outer join to return at least one of every geo

        Return:
            data: copy of dataframe
        """
        data = data.copy()
        if not hasattr(self,"stcode_cross"):
            self.load_state_cross()
        stcode_cross = self.stcode_cross[['st_code','state_id']].rename(columns={'state_id': state_id_col})
        if full:
            data = data.merge(stcode_cross, left_on=stcode_col, right_on='st_code', how='outer')
        else:
            data = data.merge(stcode_cross, left_on=stcode_col, right_on='st_code', how='left')
        return data

    def convert_fips_to_state_id(self,
                                 data,
                                 fips_col='fips',
                                 state_id_col='state_id',
                                 full=False):
        """create state_id column from county (fips) column

        Args:
            data: pd.DataFrame input data
            fips_col: fips column to convert
            state_id_col: state_id column to create
            full: boolean, if True outer join to return at least one of every geo

        Return:
            data: copy of dataframe
        """

        data = self.convert_fips_to_stcode(data,fips_col=fips_col)
        data = self.convert_stcode_to_state_id(data,state_id_col=state_id_col,full=full)
        return data

    def convert_zip_to_fips(self,
                            data,
                            zip_col="zip",
                            fips_col="fips",
                            weight_col="weight",
                            full=False):
        """create fips (county) column from zip column

        Args:
            data: pd.DataFrame input data
            zip_col: zip5 column to convert
            fips_col: fips column to create
            weight_col: weight (pop) column to create
            full: boolean, if True outer join to return at least one of every geo

        Return:
            data: copy of dataframe
        """

        data = data.copy()
        if not hasattr(self,"zip_fips_cross"):
            self.load_zip_fips_cross()
        if data[zip_col].dtype != 'O':
            data = GeoMapper.convert_int_to_str5(data,int_col=zip_col,str_col=zip_col)
        zip_cross = self.zip_fips_cross.rename(columns={'fips': fips_col, 'weight':weight_col})
        if full:
            data = data.merge(zip_cross, left_on=zip_col, right_on='zip', how='outer')
        else:
            data = data.merge(zip_cross, left_on=zip_col, right_on='zip', how='left')
        return data


    def convert_fips_to_msa(self,
                                 data,
                                 fips_col='fips',
                                 msa_col='msa',
                                 full=False):
        """create msa column from county (fips) column

        Args:
            data: pd.DataFrame input data
            fips_col: fips column to convert
            msa_col: msa column to create
            full: boolean, if True outer join to return at least one of every geo

        Return:
            data: copy of dataframe
        """

        data = data.copy()
        if not hasattr(self,"fips_msa_cross"):
            self.load_fips_msa_cross()
        if data[fips_col].dtype != 'O':
            data = self.convert_intfips_to_str(data, intfips_col=fips_col, strfips_col=fips_col)
        msa_cross = self.fips_msa_cross.rename(columns={'msa': msa_col})
        if full:
            data = data.merge(msa_cross, left_on=fips_col, right_on='fips', how='outer')
        else:
            data = data.merge(msa_cross, left_on=fips_col, right_on='fips', how='left')
        return data

    def county_to_state(self,
                      data,
                      fips_col='fips',
                      date_col='date',
                      count_cols=None,
                      full=False,
                      state_id_col="state_id"):
        """convert and aggregate from county to state_id

        Args:
            data: pd.DataFrame input data
            fips_col: fips (county) column to convert
            date_col: date column (is not aggregated)
            count_cols: the count data columns to aggregate, if None (default) all non data/geo are used
            state_id_col: state_id column to create
            full: boolean, if True outer join to return at least one of every geo

        Return:
            data: copy of dataframe
        """

        if count_cols:
            data=data[[fips_col,date_col] + count_cols].copy()
        data = self.convert_fips_to_state_id(data,fips_col=fips_col,state_id_col=state_id_col,full=full)
        data.drop([fips_col,'st_code'],axis=1,inplace=True)
        assert not data[state_id_col].isnull().values.any(), "nan states, probably invalid fips"
        if date_col:
            assert not data[date_col].isnull().values.any(), "nan dates not allowed"
            data.fillna(0,inplace=True)
            data = data.groupby([date_col,state_id_col]).sum()
        else:
            data.fillna(0,inplace=True)
            data = data.groupby(state_id_col).sum()
        return data.reset_index()

    def county_to_msa(self,
                      data,
                      fips_col='fips',
                      date_col='date',
                      count_cols=None,
                      full=False,
                      msa_col="msa"):
        """convert and aggregate from county to metropolitan statistical area (msa)

        Args:
            data: pd.DataFrame input data
            fips_col: fips (county) column to convert
            date_col: date column (is not aggregated)
            count_cols: the count data columns to aggregate, if None (default) all non data/geo are used
            msa_col: msa column to create
            full: boolean, if True outer join to return at least one of every geo

        Return:
            data: copy of dataframe
        """

        if count_cols:
            data=data[[fips_col,date_col] + count_cols].copy()
        data = self.convert_fips_to_msa(data,fips_col=fips_col,msa_col=msa_col,full=full)
        data.drop(fips_col,axis=1,inplace=True)
        if date_col:
            assert not data[date_col].isnull().values.any(), "nan dates not allowed"
            data.fillna(0,inplace=True)
            data = data.groupby([date_col,msa_col]).sum()
        else:
            data.fillna(0,inplace=True)
            data = data.groupby(msa_col).sum()
        return data.reset_index()

    def zip_to_county(self,
                      data,
                      zip_col='zip',
                      fips_col='fips',
                      date_col='date',
                      count_cols=None,
                      full=False):
        """convert and aggregate from zip to fips (county)

        Args:
            data: pd.DataFrame input data
            zip_col: zip column to convert
            fips_col: fips (county) column to create
            date_col: date column (is not aggregated, groupby), if None then no dates
            count_cols: the count data columns to aggregate, if None (default) all non data/geo are used
            full: boolean, if True outer join to return at least one of every geo

        Return:
            data: copy of dataframe
        """
        if date_col:
            assert date_col in data.columns, f'{date_col} not in data.columns'
        assert zip_col in data.columns, f'{zip_col} not in data.columns'
        if not count_cols:
            count_cols = list(set(data.columns) - {date_col, zip_col})
        else:
            count_cols = list(count_cols)
        if date_col:
            data = data[[zip_col, date_col] + count_cols].copy()
        else:
            data = data[[zip_col] + count_cols].copy()
        data = self.convert_zip_to_fips(data,zip_col=zip_col,fips_col=fips_col,full=full)
        data[count_cols] = data[count_cols].multiply(data['weight'],axis=0)
        data.drop([zip_col,'weight'],axis=1,inplace=True)
        assert not data[fips_col].isnull().values.any(), "nan fips, zip not in cross table"
        if date_col:
            assert not data[date_col].isnull().values.any(), "nan dates not allowed"
            data.fillna(0,inplace=True)
            data = data.groupby([date_col,fips_col]).sum()
        else:
            data.fillna(0,inplace=True)
            data = data.groupby(fips_col).sum()
        return data.reset_index()

    @staticmethod
    def convert_fips_to_mega(data,
                             fips_col='fips',
                             mega_col='megafips'):
        """convert fips string to a megafips string"""
        data = data.copy()
        data[mega_col] = data[fips_col].astype(str).str.zfill(5)
        data[mega_col] = data[mega_col].str.slice_replace(start=2,stop=5,repl='000')
        return data

    @staticmethod
    def megacounty_creation(data,
                            thr_count,
                            thr_win_len,
                            thr_col='visits',
                            fips_col='fips',
                            date_col='date',
                            mega_col='megafips'):
        """create megacounty column

        Args:
            data: pd.DataFrame input data
            thr_count: numeric, if the sum of counts exceed this, then fips is converted to mega
            thr_win_len: int, the number of Days to use as an average
            thr_col: str, column to use for threshold
            fips_col: str, fips (county) column to create
            date_col: str, date column (is not aggregated, groupby), if None then no dates
            mega_col: str, the megacounty column to create

        Return:
            data: copy of dataframe
        """

        assert '_thr_col_roll' not in data.columns, "column name '_thr_col_roll' is reserved"
        def agg_sum_iter(data):
            data_gby = data[[fips_col, date_col, thr_col]].set_index(date_col).groupby(fips_col)
            for _, subdf in data_gby:
                subdf_roll = subdf[thr_col].rolling(f'{thr_win_len}D').sum()
                subdf['_thr_col_roll'] = subdf_roll
                yield subdf

        data_roll = pd.concat(agg_sum_iter(data))
        data_roll.reset_index(inplace=True)
        data_roll = GeoMapper.convert_fips_to_mega(data_roll,fips_col=fips_col,mega_col=mega_col)
        data_roll.loc[data_roll['_thr_col_roll'] > thr_count,mega_col] = data_roll.loc[data_roll['_thr_col_roll'] > thr_count, fips_col]
        return data_roll.set_index([fips_col,date_col])[mega_col]

    def county_to_megacounty(self,
                             data,
                             thr_count,
                             thr_win_len,
                             thr_col='visits',
                             fips_col='fips',
                             date_col='date',
                             mega_col='megafips',
                             count_cols=None):
        """convert and aggregate from zip to fips (county)

        Args:
            data: pd.DataFrame input data
            thr_count: numeric, if the sum of counts exceed this, then fips is converted to mega
            thr_win_len: int, the number of Days to use as an average
            thr_col: str, column to use for threshold
            fips_col: str, fips (county) column to create
            date_col: str, date column (is not aggregated, groupby), if None then no dates
            mega_col: str, the megacounty column to create
            count_cols: list, the count data columns to aggregate, if None (default) all non data/geo are used

        Return:
            data: copy of dataframe
        """

        data = data.copy()
        if count_cols:
            data=data[[fips_col,date_col] + count_cols]
        if data[fips_col].dtype != 'O':
            data = self.convert_intfips_to_str(data, intfips_col=fips_col, strfips_col=fips_col)
        mega_data = GeoMapper.megacounty_creation(data,thr_count,thr_win_len,thr_col=thr_col,fips_col=fips_col,date_col=date_col,mega_col=mega_col)
        data.set_index([fips_col, date_col],inplace=True)
        data = data.join(mega_data)
        data = data.reset_index().groupby([date_col,mega_col]).sum()
        return data.reset_index()

    # def county_to_megacounty(self,
    #                          data,
    #                          thr_col,
    #                          count_cols,
    #                          threshold_visits=Config.MIN_DEN,
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
