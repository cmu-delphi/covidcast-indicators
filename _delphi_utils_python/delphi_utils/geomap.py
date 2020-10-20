"""Contains geographic mapping tools.

NOTE: This file is mostly duplicated in the Quidel pipeline; bugs fixed here
should be fixed there as well.

Author: James Sharpnack @jsharpna
Partially based on code by Maria Jahja
Created: 2020-06-01

TODO:
- remove full
- add fast cross mappings
- fix docstrings
- remove date==None
"""

from os.path import join

import pandas as pd
import pkg_resources
from os import path

DATA_PATH = "data"
ZIP_FIPS_FILE = "zip_fips_cross.csv"
STATE_FILE = "state_codes.csv"
MSA_FILE = "fips_msa_cross.csv"
JHU_FIPS_FILE = "jhu_fips_cross.csv"
FIPS_HRR_FILE = "fips_hrr_cross.csv"
ZIP_HRR_FILE = "zip_hrr_cross.csv"
JHU_UID_FIPS_FILE = "jhu_uid_fips_cross.csv"
FIPS_ZIP_FILE = "fips_zip_cross.csv"

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
       + zip -> county : population weighted
       + county -> state : unweighted
       + county -> msa : unweighted
       + county -> megacounty
       + county -> hrr
       + county -> zip
       - zip -> state
       - zip -> msa

    Geotypes (listed by default column name):
        zip: zip5, length 5 str of 0-9 with leading 0's
        fips: county, length 5 str of 0-9 with leading 0's
        msa: metro stat area, length 5 str of 0-9 with leading 0's
        st_code: state code, str in 1-99
        state_id: state id, str in AA-ZZ
        hrr: hrr id, int 1-500
    """

    def __init__(self,
                 zip_fips_filepath=path.join(DATA_PATH,ZIP_FIPS_FILE),
                 state_filepath=path.join(DATA_PATH,STATE_FILE),
                 msa_filepath=path.join(DATA_PATH,MSA_FILE),
                 jhu_filepath=path.join(DATA_PATH,JHU_FIPS_FILE),
                 hrr_filepath=path.join(DATA_PATH,ZIP_HRR_FILE),
                 jhu_uid_filepath=path.join(DATA_PATH,JHU_UID_FIPS_FILE),
                 fips_zip_filepath=path.join(DATA_PATH, FIPS_ZIP_FILE)
                ):
        """Initialize geomapper

        Args:
            fips_filepath: location of zip->fips cross table
            state_filepath: location of state_code->state_id,name cross table
            msa_filepath: location of fips->msa cross table
        """
        self.zip_fips_filepath = zip_fips_filepath
        self.state_filepath = state_filepath
        self.msa_filepath = msa_filepath
        self.jhu_filepath = jhu_filepath
        self.hrr_filepath = hrr_filepath
        self.jhu_uid_filepath = jhu_uid_filepath
        self.fips_zip_filepath = fips_zip_filepath

    def load_zip_fips_cross(self):
        """load zip->fips cross table"""
        stream = pkg_resources.resource_stream(__name__, self.zip_fips_filepath)
        self.zip_fips_cross = pd.read_csv(stream,dtype={'zip':str,
                                          'fips':str,
                                          'weight':float})

        for col in ['fips','zip']:
            self.zip_fips_cross = GeoMapper.convert_int_to_str5(self.zip_fips_cross,int_col=col,str_col=col)

    def load_fips_zip_cross(self):
        """load zip->fips cross table"""
        stream = pkg_resources.resource_stream(__name__, self.fips_zip_filepath)
        self.fips_zip_cross = pd.read_csv(stream,dtype={'fips':str,
                                          'zip':str,
                                          'weight':float})
        for col in ['fips','zip']:
            self.fips_zip_cross = GeoMapper.convert_int_to_str5(self.fips_zip_cross,int_col=col,str_col=col)

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
        for col in ['fips','msa']:
            self.fips_msa_cross = GeoMapper.convert_int_to_str5(self.fips_msa_cross,int_col=col,str_col=col)

    def load_jhu_fips_cross(self):
        """load jhu fips->fips cross table"""
        stream = pkg_resources.resource_stream(__name__, self.jhu_filepath)
        self.jhu_fips_cross = pd.read_csv(stream, dtype={'fips_jhu': str,
                                                    'fips': str,
                                                    'weight': float})
        for col in ['fips_jhu','fips']:
            self.jhu_fips_cross = GeoMapper.convert_int_to_str5(self.jhu_fips_cross, int_col=col,str_col=col)

    def load_zip_hrr_cross(self):
        """load zip->fips cross table"""
        stream = pkg_resources.resource_stream(__name__, self.hrr_filepath)
        self.zip_hrr_cross = pd.read_csv(stream,dtype={'zip':int,
                                          'hrr':int})
        self.zip_hrr_cross = GeoMapper.convert_int_to_str5(self.zip_hrr_cross, int_col='zip', str_col='zip')
        return True

    def load_jhu_uid_fips_cross(self):
        """load jhu uid->fips cross table"""
        stream = pkg_resources.resource_stream(__name__, self.jhu_uid_filepath)
        self.jhu_uid_fips_cross = pd.read_csv(stream, dtype={'jhu_uid': int,
                                                    'fips': int,
                                                    'weight': float})
        self.jhu_uid_fips_cross = GeoMapper.convert_int_to_str5(self.jhu_uid_fips_cross, int_col='fips', str_col='fips')

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

    def convert_fips_to_stcode(self,
                               data: pd.DataFrame,
                               fips_col: str = 'fips',
                               stcode_col: str = 'st_code'):
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
        data = GeoMapper.convert_int_to_str5(data,int_col=zip_col,str_col=zip_col)
        zip_cross = self.zip_fips_cross.rename(columns={'fips': fips_col, 'weight':weight_col})
        if full:
            data = data.merge(zip_cross, left_on=zip_col, right_on='zip', how='outer')
        else:
            data = data.merge(zip_cross, left_on=zip_col, right_on='zip', how='left')
        return data

    def convert_jhu_fips_to_mega(self,
                            data,
                            jhu_col="fips_jhu",
                            mega_col="fips_jhu"):
        """create jhu mega fips (county) column from jhu fips column
        - this simply converts 900XX to XX000 -

        Args:
            data: pd.DataFrame input data
            jhu_col: JHU fips column to convert
            mega_col: fips column to create

        Return:
            data: copy of dataframe
        """
        data = data.copy()
        is_mega = data[jhu_col].astype(int) > 90000
        data = GeoMapper.convert_int_to_str5(data, int_col=jhu_col, str_col=jhu_col)
        data.loc[is_mega,mega_col] = data.loc[is_mega,jhu_col].str[-2:].str.ljust(5, '0')
        data.loc[~is_mega, mega_col] = data.loc[~is_mega,jhu_col]
        return data

    def convert_jhu_fips_to_fips(self,
                            data,
                            jhu_col="fips_jhu",
                            fips_col="fips",
                            weight_col="weight"):
        """create fips (county) column from jhu fips column

        Args:
            data: pd.DataFrame input data
            jhu_col: JHU fips column to convert
            fips_col: fips column to create
            weight_col: weight (pop) column to create

        Return:
            data: copy of dataframe
        """

        data = data.copy()
        if not hasattr(self,"jhu_fips_cross"):
            self.load_jhu_fips_cross()
        data = self.convert_jhu_fips_to_mega(data,jhu_col=jhu_col,mega_col=jhu_col)
        data = GeoMapper.convert_int_to_str5(data,int_col=jhu_col,str_col=jhu_col)
        jhu_cross = self.jhu_fips_cross.rename(columns={'fips': fips_col, 'weight':weight_col})
        data = data.merge(jhu_cross, left_on=jhu_col, right_on='fips_jhu', how='left')
        jhu_no_match = data[fips_col].isna()
        data.loc[jhu_no_match,weight_col] = 1
        data.loc[jhu_no_match,fips_col] = data.loc[jhu_no_match,jhu_col]
        return data


    def convert_fips_to_msa(self,
                                 data,
                                 fips_col='fips',
                                 msa_col='msa',
                                 create_mega=False):
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
        data = self.convert_intfips_to_str(data, intfips_col=fips_col, strfips_col=fips_col)
        msa_cross = self.fips_msa_cross.rename(columns={'msa': msa_col})
        data = data.merge(msa_cross, left_on=fips_col, right_on='fips', how='left')
        if create_mega:
            data_st = data.loc[data[msa_col].isna(),fips_col]
            data.loc[data[msa_col].isna(),msa_col] = '1' + data_st.astype(str).str[:2].str.zfill(4)
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
            data = data[[fips_col,date_col] + count_cols].copy()
        data = self.convert_fips_to_state_id(data,fips_col=fips_col,state_id_col=state_id_col,full=full)
        data.dropna(subset=[state_id_col], axis=0, inplace=True)
        data.drop([fips_col,'st_code'],axis=1,inplace=True)
        assert not data[date_col].isnull().values.any(), "nan dates not allowed"
        # data.fillna(0,inplace=True)
        data = data.groupby([date_col,state_id_col]).sum()
        return data.reset_index()

    def county_to_msa(self,
                      data,
                      fips_col='fips',
                      date_col='date',
                      count_cols=None,
                      create_mega=False,
                      msa_col="msa"):
        """convert and aggregate from county to metropolitan statistical area (msa)
        This encoding is based on the most recent Census Bureau release of CBSA (March 2020)
        All counties not mapped to MSAs have msa encoded as 000XX where XX is the fips state code
        To see how the cross table is derived look at _delphi_utils_python/data_proc/geomap/*

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
        data = self.convert_fips_to_msa(data,fips_col=fips_col,msa_col=msa_col, create_mega=create_mega)
        data.drop(fips_col,axis=1,inplace=True)
        data.dropna(axis=0,subset=[msa_col],inplace=True)
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

    def jhu_fips_to_county(self,
                      data,
                      jhu_col='fips_jhu',
                      fips_col='fips',
                      date_col='date',
                      count_cols=None):
        """convert and aggregate from zip to fips (county)

        Args:
            data: pd.DataFrame input data
            jhu_col: jhu fips column to convert
            fips_col: fips (county) column to create
            date_col: date column (is not aggregated, groupby), if None then no dates
            count_cols: the count data columns to aggregate, if None (default) all non data/geo are used

        Return:
            data: copy of dataframe
        """
        if date_col:
            assert date_col in data.columns, f'{date_col} not in data.columns'
        assert jhu_col in data.columns, f'{jhu_col} not in data.columns'
        if not count_cols:
            count_cols = list(set(data.columns) - {date_col, jhu_col})
        else:
            count_cols = list(count_cols)
        if date_col:
            data = data[[jhu_col, date_col] + count_cols].copy()
        else:
            data = data[[jhu_col] + count_cols].copy()
        data = self.convert_jhu_fips_to_fips(data,jhu_col=jhu_col,fips_col=fips_col)
        data[count_cols] = data[count_cols].multiply(data['weight'],axis=0)
        data.drop([jhu_col,'weight'],axis=1,inplace=True)
        assert not data[fips_col].isnull().values.any(), "nan fips, zip not in cross table"
        if date_col:
            assert not data[date_col].isnull().values.any(), "nan dates not allowed"
            data.fillna(0,inplace=True)
            data = data.groupby([date_col,fips_col]).sum()
        else:
            data.fillna(0,inplace=True)
            data = data.groupby(fips_col).sum()
        return data.reset_index()

    def jhu_fips_to_state(self,
                      data,
                      jhu_col='fips_jhu',
                      date_col='date',
                      count_cols=None,
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

        fips_col = "_fips_col_temp"
        data = self.jhu_fips_to_county(data, jhu_col=jhu_col, fips_col=fips_col, date_col=date_col, count_cols=count_cols)
        return self.county_to_state(data, fips_col=fips_col, date_col=date_col, count_cols=count_cols, state_id_col=state_id_col)

    def jhu_fips_to_msa(self,
                      data,
                      jhu_col='fips_jhu',
                      date_col='date',
                      count_cols=None,
                      msa_col="msa"):
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
        fips_col = "_fips_col_temp"
        data = self.jhu_fips_to_county(data, jhu_col=jhu_col, fips_col=fips_col, date_col=date_col, count_cols=count_cols)
        return self.county_to_msa(data, fips_col=fips_col, date_col=date_col, count_cols=count_cols, msa_col=msa_col)

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
        data = self.convert_intfips_to_str(data, intfips_col=fips_col, strfips_col=fips_col)
        mega_data = GeoMapper.megacounty_creation(data,thr_count,thr_win_len,thr_col=thr_col,fips_col=fips_col,date_col=date_col,mega_col=mega_col)
        data.set_index([fips_col, date_col],inplace=True)
        data = data.join(mega_data)
        data = data.reset_index().groupby([date_col,mega_col]).sum()
        return data.reset_index()

    def convert_zip_to_hrr(self,
                             data,
                             zip_col='zip',
                             hrr_col='hrr'):
        """create hrr column from zip column

        Args:
            data: pd.DataFrame input data
            zip_col: zip column to convert
            hrr_col: hrr column to create

        Return:
            data: copy of dataframe
        """

        data = data.copy()
        if not hasattr(self,"zip_hrr_cross"):
            self.load_zip_hrr_cross()
        data = self.convert_intfips_to_str(data, intfips_col=zip_col, strfips_col=zip_col)
        hrr_cross = self.zip_hrr_cross.rename(columns={'hrr': hrr_col})
        data = data.merge(hrr_cross, left_on=zip_col, right_on='zip', how='left')
        return data

    def zip_to_hrr(self,
                      data,
                      zip_col='zip',
                      hrr_col='hrr',
                      date_col='date',
                      count_cols=None):
        """convert and aggregate from zip to fips (county)

        Args:
            data: pd.DataFrame input data
            zip_col: zip column to convert
            hrr_col: hrr column to create
            date_col: date column (is not aggregated, groupby)
            count_cols: the count data columns to aggregate, if None (default) all non data/geo are used

        Return:
            data: copy of dataframe
        """
        assert date_col in data.columns, f'{date_col} not in data.columns'
        assert zip_col in data.columns, f'{zip_col} not in data.columns'
        if not count_cols:
            count_cols = list(set(data.columns) - {date_col, zip_col})
        else:
            count_cols = list(count_cols)
        data = data[[zip_col, date_col] + count_cols].copy()
        data = self.convert_zip_to_hrr(data, zip_col=zip_col, hrr_col=hrr_col)
        assert not data[date_col].isnull().values.any(), "nan dates not allowed"
        data = data.groupby([date_col,hrr_col]).sum()
        return data.reset_index()

    def convert_jhu_uid_to_fips(self,
                            data,
                            jhu_col="jhu_uid",
                            fips_col="fips",
                            weight_col="weight"):
        """create fips (county) column from jhu uid column

        Args:
            data: pd.DataFrame input data
            jhu_col: int, JHU uid column to convert
            fips_col: str, fips column to create
            weight_col: weight (pop) column to create

        Return:
            data: copy of dataframe
        """

        data = data.copy().astype({jhu_col: int})
        if not hasattr(self,"jhu_uid_fips_cross"):
            self.load_jhu_uid_fips_cross()
        jhu_cross = self.jhu_uid_fips_cross.rename(columns={'fips': fips_col, 'weight':weight_col})
        data = data.merge(jhu_cross, left_on=jhu_col, right_on='jhu_uid', how='left')
        data_states = data[jhu_col].between(84090001, 84090099)
        data.loc[data_states, fips_col] = data.loc[data_states, 'jhu_uid']\
                                               .astype(str).str[-2:].str.ljust(5, '0')
        if jhu_col != 'jhu_uid':
            data.drop(columns=['jhu_uid'], inplace=True)
        return data

    def jhu_uid_to_county(self,
                      data,
                      jhu_col='jhu_uid',
                      fips_col='fips',
                      date_col='date',
                      count_cols=None):
        """convert and aggregate from zip to fips (county)

        Args:
            data: pd.DataFrame input data
            jhu_col: jhu uid column to convert
            fips_col: fips (county) column to create
            date_col: date column (is not aggregated, groupby)
            count_cols: the count data columns to aggregate, if None (default) all non data/geo are used

        Return:
            data: copy of dataframe
        """
        assert date_col in data.columns, f'{date_col} not in data.columns'
        assert jhu_col in data.columns, f'{jhu_col} not in data.columns'
        if not count_cols:
            count_cols = list(set(data.columns) - {date_col, jhu_col})
        else:
            count_cols = list(count_cols)
        data = data[[jhu_col, date_col] + count_cols].copy()
        data = self.convert_jhu_uid_to_fips(data,jhu_col=jhu_col,fips_col=fips_col)
        data.dropna(subset=[fips_col], axis=0, inplace=True)
        data[count_cols] = data[count_cols].multiply(data['weight'],axis=0)
        data.drop([jhu_col,'weight'],axis=1,inplace=True)
        assert not data[date_col].isnull().values.any(), "nan dates not allowed"
        data = data.groupby([date_col,fips_col]).sum()
        return data.reset_index()

    def convert_fips_to_zip(self,
                            data,
                            fips_col="fips",
                            zip_col="zip",
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
        if not hasattr(self,"fips_zip_cross"):
            self.load_fips_zip_cross()
        data = GeoMapper.convert_int_to_str5(data,int_col=fips_col,str_col=fips_col)
        cross = self.fips_zip_cross.rename(columns={'zip': zip_col, 'weight':weight_col})
        data = data.merge(cross, left_on=fips_col, right_on='fips', how='left')
        data.dropna(subset=[zip_col],inplace=True)
        return data

    def county_to_zip(self,
                      data,
                      fips_col='fips',
                      date_col='date',
                      count_cols=None,
                      zip_col="zip"):
        """convert and aggregate from county to zip

        Args:
            data: pd.DataFrame input data
            fips_col: fips (county) column to convert
            date_col: date column (is not aggregated)
            count_cols: the count data columns to aggregate, if None (default) all non data/geo are used
            zip_col: msa column to create

        Return:
            data: copy of dataframe
        """

        if not count_cols:
            count_cols = list(set(data.columns) - {date_col, fips_col})
        else:
            count_cols = list(count_cols)
        data = self.convert_fips_to_zip(data,fips_col=fips_col,zip_col=zip_col)
        data.drop(fips_col,axis=1,inplace=True)
        # data.dropna(axis=0,subset=[zip_col],inplace=True) - redundant
        assert not data[date_col].isnull().values.any(), "nan dates not allowed"
        data.fillna(0,inplace=True)
        data[count_cols] = data[count_cols].multiply(data['weight'],axis=0)
        data.drop('weight', axis=1, inplace=True)
        data = data.groupby([date_col, zip_col]).sum()
        return data.reset_index()

    def county_to_hrr(self,
                      data,
                      fips_col='fips',
                      date_col='date',
                      count_cols=None,
                      hrr_col="hrr"):
        """convert and aggregate from county to hrr

        Args:
            data: pd.DataFrame input data
            fips_col: fips (county) column to convert
            date_col: date column (is not aggregated)
            count_cols: the count data columns to aggregate, if None (default) all non data/geo are used
            hrr_col: hrr column to create

        Return:
            data: copy of dataframe
        """

        zip_col = "_zip_col_temp"
        data = self.county_to_zip(data, fips_col=fips_col, zip_col=zip_col, date_col=date_col, count_cols=count_cols)
        data = self.zip_to_hrr(data, zip_col=zip_col, date_col=date_col, count_cols=count_cols, hrr_col=hrr_col)
        return data.astype(dtype={hrr_col: int})
