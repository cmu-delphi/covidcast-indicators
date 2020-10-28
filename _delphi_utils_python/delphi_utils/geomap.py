"""Contains geographic mapping tools.

Authors: Dmitry Shemetov @dshemetov, James Sharpnack @jsharpna, Maria Jahja
Created: 2020-06-01

TODO:
- use a caching utility to store the crossfiles
  see: https://github.com/cmu-delphi/covidcast-indicators/issues/282
- remove deprecated functions once integration into JHU and Quidel is refactored
  see: https://github.com/cmu-delphi/covidcast-indicators/issues/283
"""

from os.path import join
import warnings
import pkg_resources

import pandas as pd
from pandas.api.types import is_string_dtype

DATA_PATH = "data"
CROSSWALK_FILEPATHS = {
    "zip": {
        "fips": join(DATA_PATH, "zip_fips_table.csv"),
        "hrr": join(DATA_PATH, "zip_hrr_table.csv"),
        "msa": join(DATA_PATH, "zip_msa_table.csv"),
        "pop": join(DATA_PATH, "zip_pop.csv"),
        "state": join(DATA_PATH, "zip_state_code_table.csv"),
    },
    "fips": {
        "zip": join(DATA_PATH, "fips_zip_table.csv"),
        "hrr": join(DATA_PATH, "fips_hrr_table.csv"),
        "msa": join(DATA_PATH, "fips_msa_table.csv"),
        "pop": join(DATA_PATH, "fips_pop.csv"),
        "state": join(DATA_PATH, "fips_state_table.csv"),
    },
    "state": {"state": join(DATA_PATH, "state_codes_table.csv")},
    "state_code": {
        "hhs_region_number": join(DATA_PATH, "state_code_hhs_region_number_table.csv")
    },
    "jhu_uid": {"fips": join(DATA_PATH, "jhu_uid_fips_table.csv")},
}


class GeoMapper:
    """Geo mapping tools commonly used in Delphi.

    The GeoMapper class provides utility functions for translating between different
    geocodes. Supported geocodes:
    - zip: zip5, a length 5 str of 0-9 with leading 0's
    - fips: state code and county code, a length 5 str of 0-9 with leading 0's
    - msa: metropolitan statistical area, a length 5 str of 0-9 with leading 0's
    - state_code: state code, a str of 0-9
    - state_id: state id, a str of A-Z
    - hrr: hospital referral region, an int 1-500

    Mappings:
    - [x] zip -> fips : population weighted
    - [x] zip -> hrr : unweighted
    - [x] zip -> msa : unweighted
    - [x] zip -> state
    - [x] zip -> population
    - [x] state code -> hhs_region_number
    - [x] fips -> state : unweighted
    - [x] fips -> msa : unweighted
    - [x] fips -> megacounty
    - [x] fips -> hrr
    - [x] nation
    - [ ] zip -> dma (postponed)

    The GeoMapper instance loads crosswalk tables from the package data_dir. The
    crosswalk tables are assumed to have been built using the geo_data_proc.py script
    in data_proc/geomap. If a mapping between codes is NOT one to many, then the table has
    just two colums. If the mapping IS one to many, then a third column, the weight column,
    exists (e.g. zip, fips, weight; satisfying (sum(weights) where zip==ZIP) == 1).

    Example Usage
    ==========
    The main GeoMapper object loads and stores crosswalk dataframes on-demand.

    Example 1: to add a new column with a new geocode, possibly with weights:
    > gmpr = GeoMapper()
    > df = gmpr.add_geocode(df, "fips", "zip", from_col="fips", new_col="geo_id",
                            date_col="timestamp", dropna=False)

    Example 2: to replace a geocode column with a new one, aggregating the data with weights:
    > gmpr = GeoMapper()
    > df = gmpr.replace_geocode(df, "fips", "zip", from_col="fips", new_col="geo_id",
                                date_col="timestamp", dropna=False)
    """

    def __init__(self):
        """Initialize geomapper. Holds loading the crosswalk tables
        until a conversion function is first used.

        Parameters
        ---------
        crosswalk_files : dict
            A dictionary of the filenames for the crosswalk tables.
        """
        self.crosswalk_filepaths = CROSSWALK_FILEPATHS
        self.crosswalks = {
            "zip": {"fips": None, "hrr": None, "msa": None, "pop": None, "state": None},
            "fips": {"zip": None, "hrr": None, "msa": None, "pop": None, "state": None},
            "state": {"state": None},
            "state_code": {"hhs_region_number": None},
            "jhu_uid": {"fips": None},
        }

    # Utility functions
    def _load_crosswalk(self, from_code, to_code):
        """Loads the crosswalk from from_code -> to_code."""
        stream = pkg_resources.resource_stream(
            __name__, self.crosswalk_filepaths[from_code][to_code]
        )
        if self.crosswalks[from_code][to_code] is None:
            # Weighted crosswalks
            if (from_code, to_code) in [
                ("zip", "fips"),
                ("fips", "zip"),
                ("jhu_uid", "fips"),
                ("zip", "msa"),
                ("fips", "hrr"),
            ]:
                self.crosswalks[from_code][to_code] = pd.read_csv(
                    stream,
                    dtype={
                        from_code: str,
                        to_code: str,
                        "weight": float,
                    },
                )
            # Unweighted crosswalks
            elif (from_code, to_code) in [
                ("zip", "hrr"),
                ("fips", "msa"),
            ]:
                self.crosswalks[from_code][to_code] = pd.read_csv(
                    stream,
                    dtype={from_code: str, to_code: str},
                )
            # Special table of state codes, state IDs, and state names
            elif (from_code, to_code) == ("state", "state"):
                self.crosswalks[from_code][to_code] = pd.read_csv(
                    stream,
                    dtype={
                        "state_code": str,
                        "state_id": str,
                        "state_name": str,
                    },
                )
            elif (from_code, to_code) == ("state_code", "hhs_region_number"):
                self.crosswalks[from_code][to_code] = pd.read_csv(
                    stream,
                    dtype={"state_code": str, "hhs_region_number": str},
                )
            elif (from_code, to_code) == ("zip", "state"):
                self.crosswalks[from_code][to_code] = pd.read_csv(
                    stream,
                    dtype={
                        "zip": str,
                        "weight": float,
                        "state_code": str,
                        "state_id": str,
                        "state_name": str,
                    },
                )
            elif (from_code, to_code) == ("fips", "state"):
                self.crosswalks[from_code][to_code] = pd.read_csv(
                    stream,
                    dtype={
                        "fips": str,
                        "state_code": str,
                        "state_id": str,
                        "state_name": str,
                    },
                )
            # Population tables
            elif (from_code, to_code) in [("fips", "pop"), ("zip", "pop")]:
                self.crosswalks[from_code][to_code] = pd.read_csv(
                    stream,
                    dtype={
                        from_code: str,
                        "pop": int,
                    },
                )
        return self.crosswalks[from_code][to_code]

    @staticmethod
    def convert_fips_to_mega(data, fips_col="fips", mega_col="megafips"):
        """convert fips string to a megafips string"""
        data = data.copy()
        data[mega_col] = data[fips_col].astype(str).str.zfill(5)
        data[mega_col] = data[mega_col].str.slice_replace(start=2, stop=5, repl="000")
        return data

    @staticmethod
    def megacounty_creation(
        data,
        thr_count,
        thr_win_len,
        thr_col="visits",
        fips_col="fips",
        date_col="date",
        mega_col="megafips",
    ):
        """create megacounty column

        Parameters
        ---------
        data: pd.DataFrame input data
        thr_count: numeric, if the sum of counts exceed this, then fips is converted to mega
        thr_win_len: int, the number of Days to use as an average
        thr_col: str, column to use for threshold
        fips_col: str, fips (county) column to create
        date_col: str, date column (is not aggregated, groupby), if None then no dates
        mega_col: str, the megacounty column to create

        Return
        ---------
        data: copy of dataframe
            A dataframe with a new column, mega_col, that contains megaFIPS (aggregate
            of FIPS clusters) values depending on the number of data samples available.
        """
        if "_thr_col_roll" in data.columns:
            raise ValueError("Column name '_thr_col_roll' is reserved.")

        def agg_sum_iter(data):
            data_gby = (
                data[[fips_col, date_col, thr_col]]
                .set_index(date_col)
                .groupby(fips_col)
            )
            for _, subdf in data_gby:
                subdf_roll = subdf[thr_col].rolling(f"{thr_win_len}D").sum()
                subdf["_thr_col_roll"] = subdf_roll
                yield subdf

        data_roll = pd.concat(agg_sum_iter(data))
        data_roll.reset_index(inplace=True)
        data_roll = GeoMapper.convert_fips_to_mega(
            data_roll, fips_col=fips_col, mega_col=mega_col
        )
        data_roll.loc[data_roll["_thr_col_roll"] > thr_count, mega_col] = data_roll.loc[
            data_roll["_thr_col_roll"] > thr_count, fips_col
        ]
        return data_roll.set_index([fips_col, date_col])[mega_col]

    # Conversion functions
    def add_geocode(
        self, df, from_code, new_code, from_col=None, new_col=None, dropna=True
    ):
        """Add a new geocode column to a dataframe.

        Currently supported conversions:
        - fips -> state_code, state_id, state_name, zip, msa, hrr, nation
        - zip -> state_code, state_id, state_name, fips, msa, hrr, nation
        - jhu_uid -> fips
        - state_x -> state_y, where x and y are in {code, id, name}
        - state_code -> hhs_region_number

        Parameters
        ---------
        df: pd.DataFrame
            Input dataframe.
        from_code: {'fips', 'zip', 'jhu_uid', 'state_code', 'state_id', 'state_name'}
            Specifies the geocode type of the data in from_col.
        new_code: {'fips', 'zip', 'state_code', 'state_id', 'state_name', 'hrr', 'msa',
                   'hhs_region_number'}
            Specifies the geocode type in new_col.
        from_col: str, default None
            Name of the column in dataframe containing from_code. If None, then the name
            is assumed to be from_code.
        new_col: str, default None
            Name of the column in dataframe containing new_code. If None, then the name
            is assumed to be new_code.
        dropna: bool, default False
            Determines how the merge with the crosswalk file is done. If True, the join is inner,
            and if False, the join is left. The inner join will drop records from the input database
            that have no translation in the crosswalk, while the outer join will keep those records
            as NA.

        Return
        ---------
        df: pd.DataFrame
            A copy of the dataframe with a new geocode column added.
        """
        df = df.copy()
        from_col = from_code if from_col is None else from_col
        new_col = new_code if new_col is None else new_col
        state_codes = ["state_code", "state_id", "state_name"]

        if not is_string_dtype(df[from_col]):
            if from_code in ["fips", "zip"]:
                df[from_col] = df[from_col].astype(str).str.zfill(5)
            else:
                df[from_col] = df[from_col].astype(str)

        # Assuming that the passed-in records are all United States data, at the moment
        if (from_code, new_code) in [("fips", "nation"), ("zip", "nation")]:
            df[new_col] = df[from_col].apply(lambda x: "us")
            return df
        elif new_code == "nation":
            raise ValueError(
                "Conversion to the nation level is only supported from the FIPS and ZIP codes."
            )

        # state codes are all stored in one table
        if new_code in state_codes:
            crosswalk = self._load_crosswalk(from_code=from_code, to_code="state")
            crosswalk = crosswalk.rename(
                columns={from_code: from_col, new_code: new_col}
            )
        else:
            crosswalk = self._load_crosswalk(from_code=from_code, to_code=new_code)
            crosswalk = crosswalk.rename(
                columns={from_code: from_col, new_code: new_col}
            )

        if dropna:
            df = df.merge(crosswalk, left_on=from_col, right_on=from_col, how="inner")
        else:
            df = df.merge(crosswalk, left_on=from_col, right_on=from_col, how="left")

        # Drop extra state columns
        if new_code in state_codes:
            state_codes.remove(new_code)
            df.drop(columns=state_codes, inplace=True)

        return df

    def replace_geocode(
        self,
        df,
        from_code,
        new_code,
        from_col=None,
        new_col=None,
        date_col="date",
        data_cols=None,
        dropna=True,
    ):
        """Replace a geocode column in a dataframe.

        Currently supported conversions:
        - fips -> state_code, state_id, state_name, zip, msa, hrr, nation
        - zip -> state_code, state_id, state_name, fips, msa, hrr, nation
        - jhu_uid -> fips
        - state_x -> state_y, where x and y are in {code, id, name}
        - state_code -> hhs_region_number

        Parameters
        ---------
        df: pd.DataFrame
            Input dataframe.
        from_col: str
            Name of the column in data to match and remove.
        from_code: {'fips', 'zip', 'jhu_uid', 'state_code', 'state_id', 'state_name'}
            Specifies the geocode type of the data in from_col.
        new_col: str
            Name of the new column to add to data.
        new_code: {'fips', 'zip', 'state_code', 'state_id', 'state_name', 'hrr', 'msa',
                   'hhs_region_number'}
            Specifies the geocode type of the data in new_col.
        data_cols: list, default None
            A list of data column names to aggregate when doing a weighted coding. If set to
            None, then all the columns are used except for date_col and new_col.
        dropna: bool, default False
            Determines how the merge with the crosswalk file is done. If True, the join is inner,
            and if False, the join is left. The inner join will drop records from the input database
            that have no translation in the crosswalk, while the outer join will keep those records
            as NA.

        Return
        ---------
        df: pd.DataFrame
            A copy of the dataframe with a new geocode column replacing the old.
        """
        from_col = from_code if from_col is None else from_col
        new_col = new_code if new_col is None else new_col

        df = self.add_geocode(
            df, from_code, new_code, from_col=from_col, new_col=new_col, dropna=dropna
        ).drop(columns=from_col)

        if "weight" in df.columns:
            if data_cols is None:
                data_cols = list(set(df.columns) - {date_col, new_col, "weight"})

            # Multiply and aggregate (this automatically zeros NAs)
            df[data_cols] = df[data_cols].multiply(df["weight"], axis=0)
            df.drop("weight", axis=1, inplace=True)
        df = df.groupby([date_col, new_col]).sum().reset_index()
        return df

    def add_population_column(self, data, geocode_type, geocode_col=None, dropna=True):
        """
        Appends a population column to a dataframe, based on the FIPS or ZIP code. If no
        dataframe is provided, the full crosswalk from geocode to population is returned.

        Parameters
        ---------
        data: pd.DataFrame
            The dataframe with a FIPS code column.
        geocode_type: {"fips", "zip"}
            The type of the geocode contained in geocode_col.
        geocode_col: str, default None
            The name of the column containing the geocodes. If None, uses the geocode_type
            as the name.

        Returns
        --------
        data_with_pop: pd.Dataframe
            A dataframe with a population column appended.
        """
        geocode_col = geocode_type if geocode_col is None else geocode_col
        data = data.copy()

        if geocode_type not in ["fips", "zip"]:
            raise ValueError(
                "Only fips and zip geocodes supported. \
                For other codes, aggregate those."
            )

        pop_df = self._load_crosswalk(from_code=geocode_type, to_code="pop")

        if not is_string_dtype(data[geocode_col]):
            data[geocode_col] = data[geocode_col].astype(str).str.zfill(5)

        merge_type = "inner" if dropna else "left"
        data_with_pop = (
            data
            .merge(pop_df, left_on=geocode_col, right_on=geocode_type, how=merge_type)
            .rename(columns={"pop": "population"})
        )

        return data_with_pop

    @staticmethod
    def fips_to_megacounty(
        data,
        thr_count,
        thr_win_len,
        thr_col="visits",
        fips_col="fips",
        date_col="date",
        mega_col="megafips",
        count_cols=None,
    ):
        """Convert and aggregate from FIPS to megaFIPS

        Parameters
        ---------
            data: pd.DataFrame input data
            thr_count: numeric, if the sum of counts exceed this, then fips is converted to mega
            thr_win_len: int, the number of Days to use as an average
            thr_col: str, column to use for threshold
            fips_col: str, fips (county) column to create
            date_col: str, date column (is not aggregated, groupby), if None then no dates
            mega_col: str, the megacounty column to create
            count_cols: list, the count data columns to aggregate, if None (default) all non
                        data/geo are used

        Return
        ---------
            data: copy of dataframe
                A dataframe with data aggregated into megaFIPS codes (aggregate
                of FIPS clusters) values depending on the number of data samples available.
        """
        data = data.copy()
        if count_cols:
            data = data[[fips_col, date_col] + count_cols]

        if not is_string_dtype(data[fips_col]):
            data[fips_col] = data[fips_col].astype(str).str.zfill(5)

        mega_data = GeoMapper.megacounty_creation(
            data,
            thr_count,
            thr_win_len,
            thr_col=thr_col,
            fips_col=fips_col,
            date_col=date_col,
            mega_col=mega_col,
        )
        data.set_index([fips_col, date_col], inplace=True)
        data = data.join(mega_data)
        data = data.reset_index().groupby([date_col, mega_col]).sum()
        return data.reset_index()

    ### DEPRECATED FUNCTIONS BELOW

    def convert_fips_to_state_code(
        self, data, fips_col="fips", state_code_col="state_code"
    ):
        """DEPRECATED
        Add a state_code column to a dataframe with fips column.

        Parameters
        ---------
        data: pd.DataFrame
            Input dataframe.
        fips_col: str
            Name of FIPS column to convert in data.
        state_code_col: str
            Name of State Code column to convert in data.

        Return
        ---------
        data: pd.DataFrame
            A copy of the dataframe with a state code column added.
        """
        warnings.warn(
            "Use the function add_geocode(df, 'fips', 'state_code', ...) instead.",
            DeprecationWarning,
        )

        data = data.copy()

        if not is_string_dtype(data[fips_col]):
            data[fips_col] = data[fips_col].astype(str).str.zfill(5)

        # Take the first two digits of the FIPS code
        data[state_code_col] = data[fips_col].str[:2]

        return data

    def fips_to_state_code(
        self,
        data,
        fips_col="fips",
        date_col="date",
        count_cols=None,
        state_code_col="state_code",
    ):
        """DEPRECATED
        Translate dataframe from fips to state.

        Parameters
        ---------
        data: pd.DataFrame
            Input data.
        fips_col: str
            Name of dataframe column containing fips codes.
        date_col: str
            Name of dataframe column containing the dates.
        count_cols: str
            Name of dataframe column containing the data. If None (default)
            all non fips/date are used.
        state_id_col: str
            Name of dataframe column to contain the state codes.

        Return
        ---------
        data: pd.DataFrame
            A new dataframe with fips converted to state.
        """
        warnings.warn(
            "Use the function replace_geocode(df, 'fips', 'state_code', ...) instead.",
            DeprecationWarning,
        )

        if count_cols:
            data = data[[fips_col, date_col] + count_cols].copy()
        data = self.convert_fips_to_state_code(
            data, fips_col=fips_col, state_code_col=state_code_col
        )
        data = data.groupby([date_col, state_code_col]).sum()
        return data.reset_index()

    def convert_fips_to_state_id(self, data, fips_col="fips", state_id_col="state_id"):
        """DEPRECATED
        Create State ID column from FIPS column.

        Parameters
        ---------
        data: pd.DataFrame
            Input dataframe.
        fips_col: str
            Name of FIPS column to convert in data.
        state_id_col: str
            Name of State ID column to convert in data.

        Return
        ---------
        data: pd.DataFrame
            A copy of the dataframe with a state code column added.
        """
        warnings.warn(
            "Use the function add_geocode(df, 'fips', 'state_id', ...) instead.",
            DeprecationWarning,
        )

        data = self.convert_fips_to_state_code(data, fips_col=fips_col)
        data = self.convert_state_code_to_state_id(data, state_id_col=state_id_col)
        return data

    def convert_fips_to_msa(
        self, data, fips_col="fips", msa_col="msa", create_mega=False
    ):
        """DEPRECATED
        Translate dataframe from fips to msa.

        Parameters
        ---------
        data: pd.DataFrame
            Input data.
        fips_col: str
            Name of dataframe column containing fips codes.
        date_col: str
            Name of dataframe column containing the dates.
        count_cols: str
            Name of dataframe column containing the data. If None (default) all
            non fips/date are used.
        msa_col: str
            Name of dataframe column to contain the msa codes.

        Return
        ---------
        data: pd.DataFrame
            A new dataframe with fips converted to msa.
        """
        warnings.warn(
            "Use the function add_geocode(df, 'fips', 'msa', ...) instead.",
            DeprecationWarning,
        )

        df = self._load_crosswalk(from_code="fips", to_code="msa")
        data = data.copy()

        if not is_string_dtype(data[fips_col]):
            data[fips_col] = data[fips_col].astype(str).str.zfill(5)

        msa_table = df.rename(columns={"msa": msa_col})
        data = data.merge(msa_table, left_on=fips_col, right_on="fips", how="left")

        # Megacounty codes are 1, followed by up to 4 leading zeros, and ending with
        # two digits of the state's FIPS code.=
        if create_mega:
            data_st = data.loc[data[msa_col].isna(), fips_col]
            data.loc[data[msa_col].isna(), msa_col] = "1" + data_st.astype(str).str[
                :2
            ].str.zfill(4)

        return data

    def convert_fips_to_zip(
        self, data, fips_col="fips", zip_col="zip", weight_col="weight"
    ):
        """DEPRECATED
        Create ZIP column from FIPS column.

        Parameters
        ---------
        data: pd.DataFrame
            Input data.
        fips_col: str
            Name of dataframe column containing fips codes.
        zip_col: str
            Name of dataframe column containing zip codes.
        weight_col: str
            Name of dataframe weight column to create.

        Return
        ---------
        data: copy of dataframe
        """
        warnings.warn(
            "Use the function add_geocode(df, 'fips', 'zip', ...) instead.",
            DeprecationWarning,
        )

        df = self._load_crosswalk(from_code="fips", to_code="zip")
        data = data.copy()

        if not is_string_dtype(data[fips_col]):
            data[fips_col] = data[fips_col].astype(str).str.zfill(5)

        cross = df.rename(columns={"zip": zip_col, "weight": weight_col})
        data = data.merge(cross, left_on=fips_col, right_on="fips", how="left").dropna(
            subset=[zip_col]
        )
        return data

    def convert_state_code_to_state_id(
        self, data, state_code_col="state_code", state_id_col="state_id"
    ):
        """DEPRECATED
        create state_id column from state_code column

        Parameters
        ---------
        data: pd.DataFrame input data
        state_code_col: state_code column to convert
        state_id_col: state_id column to create

        Return
        ---------
        data: copy of dataframe
        """
        warnings.warn(
            "Use the function add_geocode(df, 'state_code', 'state_id', ...) instead.",
            DeprecationWarning,
        )

        state_table = self._load_crosswalk(from_code="state", to_code="state")
        state_table = state_table[["state_code", "state_id"]].rename(
            columns={"state_id": state_id_col}
        )
        data = data.copy()

        data = data.merge(
            state_table, left_on=state_code_col, right_on="state_code", how="left"
        )
        return data

    def convert_zip_to_fips(
        self, data, zip_col="zip", fips_col="fips", weight_col="weight"
    ):
        """DEPRECATED
        Create FIPS column from ZIP column.

        Parameters
        ---------
        data: pd.DataFrame input data
        zip_col: zip5 column to convert
        fips_col: fips column to create
        weight_col: weight (pop) column to create

        Return
        ---------
        data: copy of dataframe
        """
        warnings.warn(
            "Use the function add_geocode(df, 'zip', 'fips', ...) instead.",
            DeprecationWarning,
        )

        df = self._load_crosswalk(from_code="zip", to_code="fips")
        data = data.copy()

        if not is_string_dtype(data[zip_col]):
            data[zip_col] = data[zip_col].astype(str).str.zfill(5)

        zip_table = df.rename(columns={"fips": fips_col, "weight": weight_col})
        data = data.merge(zip_table, left_on=zip_col, right_on="zip", how="left")
        return data

    def convert_zip_to_hrr(self, data, zip_col="zip", hrr_col="hrr"):
        """DEPRECATED
        create hrr column from zip column

        Parameters
        ---------
        data: pd.DataFrame input data
        zip_col: zip column to convert
        hrr_col: hrr column to create

        Return
        ---------
        data: copy of dataframe
        """
        warnings.warn(
            "Use the function add_geocode(df, 'zip', 'hrr', ...) instead.",
            DeprecationWarning,
        )

        df = self._load_crosswalk(from_code="zip", to_code="hrr")
        data = data.copy()

        if not is_string_dtype(data[zip_col]):
            data[zip_col] = data[zip_col].astype(str).str.zfill(5)

        hrr_table = df.rename(columns={"hrr": hrr_col})
        data = data.merge(hrr_table, left_on=zip_col, right_on="zip", how="left")
        return data

    def convert_zip_to_msa(
        self, data, zip_col="zip", msa_col="msa", date_col="date", count_cols=None
    ):
        """DEPRECATED."""
        warnings.warn(
            "Use the function add_geocode(df, 'zip', 'msa', ...) instead.",
            DeprecationWarning,
        )

        zip_to_msa_cross = self._load_crosswalk(from_code="zip", to_code="msa")
        data = data.copy()

        if count_cols:
            data = data[[zip_col, date_col] + count_cols].copy()

        if not is_string_dtype(data[zip_col]):
            data[zip_col] = data[zip_col].astype(str).str.zfill(5)

        data = data.merge(zip_to_msa_cross, left_on="zip", right_on="zip", how="left")
        return data

    def zip_to_msa(
        self, data, zip_col="zip", msa_col="msa", date_col="date", count_cols=None
    ):
        """DEPRECATED."""
        warnings.warn(
            "Use the function replace_geocode(df, 'zip', 'msa', ...) instead.",
            DeprecationWarning,
        )

        data = self.convert_zip_to_msa(
            data,
            zip_col=zip_col,
            msa_col=msa_col,
            date_col=date_col,
            count_cols=count_cols,
        )
        data.drop(columns="zip", inplace=True)

        if count_cols is None:
            count_cols = list(set(data.columns) - {date_col, msa_col, "weight"})

        data[count_cols] = data[count_cols].multiply(data["weight"], axis=0)
        data.drop("weight", axis=1, inplace=True)
        data = data.groupby([date_col, msa_col], dropna=False).sum()
        return data.reset_index()

    def convert_zip_to_state_code(
        self,
        data,
        zip_col="zip",
        state_code_col="state_code",
        date_col="date",
        count_cols=None,
    ):
        """DEPRECATED."""
        warnings.warn(
            "Use the function add_geocode(df, 'zip', 'state_code', ...) instead.",
            DeprecationWarning,
        )

        zip_to_state_cross = self._load_crosswalk(from_code="zip", to_code="state")
        zip_to_state_cross = zip_to_state_cross.drop(
            columns=["state_id", "state_name"]
        ).rename({"state_code": state_code_col})

        if count_cols:
            data = data[[zip_col, date_col] + count_cols].copy()
        else:
            data = data.copy()

        if not is_string_dtype(data[zip_col]):
            data[zip_col] = data[zip_col].astype(str).str.zfill(5)

        data = data.merge(zip_to_state_cross, left_on="zip", right_on="zip", how="left")
        return data

    def zip_to_state_code(
        self,
        data,
        zip_col="zip",
        state_code_col="state_code",
        date_col="date",
        count_cols=None,
    ):
        """DEPRECATED."""
        warnings.warn(
            "Use the function replace_geocode(df, 'zip', 'state_code', ...) instead.",
            DeprecationWarning,
        )

        data = self.convert_zip_to_state_code(
            data,
            zip_col=zip_col,
            state_code_col=state_code_col,
            date_col=date_col,
            count_cols=count_cols,
        )
        data.drop(columns="zip", inplace=True)

        if count_cols is None:
            count_cols = list(set(data.columns) - {date_col, state_code_col, "weight"})

        data[count_cols] = data[count_cols].multiply(data["weight"], axis=0)
        data.drop("weight", axis=1, inplace=True)
        data = data.groupby([date_col, state_code_col], dropna=False).sum()
        return data.reset_index()

    def convert_zip_to_state_id(
        self,
        data,
        zip_col="zip",
        state_id_col="state_id",
        date_col="date",
        count_cols=None,
    ):
        """DEPRECATED."""
        warnings.warn(
            "Use the function add_geocode(df, 'zip', 'state_id', ...) instead.",
            DeprecationWarning,
        )

        zip_to_state_cross = self._load_crosswalk(from_code="zip", to_code="state")
        zip_to_state_cross = zip_to_state_cross.drop(
            columns=["state_code", "state_name"]
        ).rename({"state_id": state_id_col})

        if count_cols:
            data = data[[zip_col, date_col] + count_cols].copy()
        else:
            data = data.copy()

        if not is_string_dtype(data[zip_col]):
            data[zip_col] = data[zip_col].astype(str).str.zfill(5)

        data = data.merge(zip_to_state_cross, left_on="zip", right_on="zip", how="left")
        return data

    def zip_to_state_id(
        self,
        data,
        zip_col="zip",
        state_id_col="state_id",
        date_col="date",
        count_cols=None,
    ):
        """DEPRECATED"""
        warnings.warn(
            "Use the function replace_geocode(df, 'zip', 'state_id', ...) instead.",
            DeprecationWarning,
        )

        data = self.convert_zip_to_state_id(
            data,
            zip_col=zip_col,
            state_id_col=state_id_col,
            date_col=date_col,
            count_cols=count_cols,
        )
        data.drop(columns="zip", inplace=True)

        if count_cols is None:
            count_cols = list(set(data.columns) - {date_col, state_id_col, "weight"})

        data[count_cols] = data[count_cols].multiply(data["weight"], axis=0)
        data.drop("weight", axis=1, inplace=True)
        data = data.groupby([date_col, state_id_col], dropna=False).sum()
        return data.reset_index()

    def fips_to_state_id(
        self,
        data,
        fips_col="fips",
        date_col="date",
        count_cols=None,
        state_id_col="state_id",
    ):
        """DEPRECATED
        Translate dataframe from fips to state.

        Parameters
        ---------
        data: pd.DataFrame
            Input data.
        fips_col: str
            Name of dataframe column containing fips codes.
        date_col: str
            Name of dataframe column containing the dates.
        count_cols: str
            Name of dataframe column containing the data. If None (default)
            all non fips/date are used.
        state_id_col: str
            Name of dataframe column to contain the state codes.

        Return
        ---------
        data: pd.DataFrame
            A new dataframe with fips converted to state.
        """
        warnings.warn(
            "Use the function replace_geocode(df, 'fips', 'state_id', ...) instead.",
            DeprecationWarning,
        )

        if count_cols:
            data = data[[fips_col, date_col] + count_cols].copy()
        data = self.convert_fips_to_state_id(
            data, fips_col=fips_col, state_id_col=state_id_col
        )
        # data.drop([fips_col, "state_code"], axis=1, inplace=True)
        data = data.groupby([date_col, state_id_col]).sum()
        return data.reset_index()

    def fips_to_msa(
        self,
        data,
        fips_col="fips",
        date_col="date",
        count_cols=None,
        create_mega=False,
        msa_col="msa",
    ):
        """DEPRECATED
        Translate dataframe from fips to metropolitan statistical area (msa).

        The encoding we use is based on the most recent Census Bureau release of CBSA (March 2020)
        All counties not mapped to MSAs have msa encoded as 000XX where XX is the fips state code
        To see how the crosswalk table is derived look at _delphi_utils_python/data_proc/geomap/*

        Parameters
        ---------
        data: pd.DataFrame
            Input data.
        fips_col: str
            Name of dataframe column containing fips codes.
        date_col: str
            Name of dataframe column containing the dates.
        count_cols: str
            Name of dataframe column containing the data. If None (default)
            all non fips/date are used.
        msa_col: str
            Name of dataframe column to contain the msa codes.

        Return
        ---------
        data: pd.DataFrame
            A new dataframe with fips converted to msa.
        """
        warnings.warn(
            "Use the function replace_geocode(df, 'fips', 'msa', ...) instead.",
            DeprecationWarning,
        )

        if count_cols:
            data = data[[fips_col, date_col] + count_cols].copy()
        data = self.convert_fips_to_msa(
            data, fips_col=fips_col, msa_col=msa_col, create_mega=create_mega
        )
        data.drop(fips_col, axis=1, inplace=True)
        data.dropna(axis=0, subset=[msa_col], inplace=True)
        if date_col:
            data = data.groupby([date_col, msa_col]).sum()
        else:
            data = data.groupby(msa_col).sum()
        return data.reset_index()

    def zip_to_fips(
        self, data, zip_col="zip", fips_col="fips", date_col="date", count_cols=None
    ):
        """DEPRECATED
        Convert and aggregate from ZIP to FIPS.

        Parameters
        ---------
        data: pd.DataFrame input data
        zip_col: zip column to convert
        fips_col: fips (county) column to create
        date_col: date column (is not aggregated, groupby), if None then no dates
        count_cols: the count data columns to aggregate, if None (default) all non data/geo are used

        Return
        ---------
        data: copy of dataframe
        """
        warnings.warn(
            "Use the function replace_geocode(df, 'zip', 'fips', ...) instead.",
            DeprecationWarning,
        )

        if date_col:
            assert date_col in data.columns, f"{date_col} not in data.columns"
        assert zip_col in data.columns, f"{zip_col} not in data.columns"
        if not count_cols:
            count_cols = list(set(data.columns) - {date_col, zip_col})
        else:
            count_cols = list(count_cols)
        if date_col:
            data = data[[zip_col, date_col] + count_cols].copy()
        else:
            data = data[[zip_col] + count_cols].copy()
        data = self.convert_zip_to_fips(data, zip_col=zip_col, fips_col=fips_col)
        data[count_cols] = data[count_cols].multiply(data["weight"], axis=0)
        data.drop([zip_col, "weight"], axis=1, inplace=True)

        if date_col:
            data = data.groupby([date_col, fips_col]).sum()
        else:
            data = data.groupby(fips_col).sum()
        return data.reset_index()

    def zip_to_hrr(
        self, data, zip_col="zip", hrr_col="hrr", date_col="date", count_cols=None
    ):
        """DEPRECATED
        Convert and aggregate from ZIP to FIPS.

        Parameters
        ---------
            data: pd.DataFrame input data
            zip_col: zip column to convert
            hrr_col: hrr column to create
            date_col: date column (is not aggregated, groupby)
            count_cols: the count data columns to aggregate, if None (default) all
                        non data/geo are used

        Return
        ---------
            data: copy of dataframe
        """
        warnings.warn(
            "Use the function replace_geocode(df, 'zip', 'hrr', ...) instead.",
            DeprecationWarning,
        )

        if count_cols is None:
            count_cols = list(set(data.columns) - {date_col, zip_col})
        else:
            count_cols = list(count_cols)

        data = data[[zip_col, date_col] + count_cols].copy()
        data = self.convert_zip_to_hrr(data, zip_col=zip_col, hrr_col=hrr_col)
        data = data.groupby([date_col, hrr_col]).sum()
        return data.reset_index()

    def convert_jhu_uid_to_fips(
        self, data, jhu_col="jhu_uid", fips_col="fips", weight_col="weight"
    ):
        """DEPRECATED
        create fips (county) column from jhu uid column

        Parameters
        ---------
        data: pd.DataFrame input data
        jhu_col: int, JHU uid column to convert
        fips_col: str, fips column to create
        weight_col: weight (pop) column to create

        Return
        ---------
        data: copy of dataframe
        """
        warnings.warn(
            "Use the function add_geocode(df, 'jhu_uid', 'fips', ...) instead.",
            DeprecationWarning,
        )

        df = self._load_crosswalk(from_code="jhu_uid", to_code="fips")
        data = data.copy()

        if not is_string_dtype(data[jhu_col]):
            data[jhu_col] = data[jhu_col].astype(str)

        jhu_table = df.rename(columns={"fips": fips_col, "weight": weight_col})
        data = data.merge(jhu_table, left_on=jhu_col, right_on="jhu_uid", how="left")
        if jhu_col != "jhu_uid":
            data.drop(columns=["jhu_uid"], inplace=True)
        return data

    def jhu_uid_to_fips(
        self, data, jhu_col="jhu_uid", fips_col="fips", date_col="date", count_cols=None
    ):
        """DEPRECATED
        Convert and aggregate from zip to fips

        Parameters
        ---------
            data: pd.DataFrame input data
            jhu_col: jhu uid column to convert
            fips_col: fips (county) column to create
            date_col: date column (is not aggregated, groupby)
            count_cols: the count data columns to aggregate, if None (default) all non
                        data/geo are used

        Return
        ---------
            data: copy of dataframe
        """
        warnings.warn(
            "Use the function replace_geocode(df, 'jhu_uid', 'fips', ...) instead.",
            DeprecationWarning,
        )

        assert date_col in data.columns, f"{date_col} not in data.columns"
        assert jhu_col in data.columns, f"{jhu_col} not in data.columns"
        if not count_cols:
            count_cols = list(set(data.columns) - {date_col, jhu_col})
        else:
            count_cols = list(count_cols)
        data = data[[jhu_col, date_col] + count_cols].copy()
        data = self.convert_jhu_uid_to_fips(data, jhu_col=jhu_col, fips_col=fips_col)
        data.dropna(subset=[fips_col], axis=0, inplace=True)
        data[count_cols] = data[count_cols].multiply(data["weight"], axis=0)
        data.drop([jhu_col, "weight"], axis=1, inplace=True)
        data = data.groupby([date_col, fips_col]).sum()
        return data.reset_index()

    def fips_to_zip(
        self, data, fips_col="fips", date_col="date", count_cols=None, zip_col="zip"
    ):
        """DEPRECATED
        Convert and aggregate from fips to zip

        Parameters
        ---------
        data: pd.DataFrame input data
        fips_col: fips (county) column to convert
        date_col: date column (is not aggregated)
        count_cols: the count data columns to aggregate, if None (default) all non data/geo are used
        zip_col: msa column to create

        Return
        ---------
        data: copy of dataframe
        """
        warnings.warn(
            "Use the function replace_geocode(df, 'fips', 'zip', ...) instead.",
            DeprecationWarning,
        )

        if not count_cols:
            count_cols = list(set(data.columns) - {date_col, fips_col})
        else:
            count_cols = list(count_cols)
        data = self.convert_fips_to_zip(data, fips_col=fips_col, zip_col=zip_col)
        data.drop(fips_col, axis=1, inplace=True)
        data[count_cols] = data[count_cols].multiply(data["weight"], axis=0)
        data.drop("weight", axis=1, inplace=True)
        data = data.groupby([date_col, zip_col]).sum()
        return data.reset_index()

    def convert_fips_to_hrr(self, data, fips_col="fips", hrr_col="hrr"):
        """DEPRECATED
        convert and aggregate from fips to hrr

        Parameters
        ---------
            data: pd.DataFrame input data
            fips_col: fips (county) column to convert
            date_col: date column (is not aggregated)
            count_cols: the count data columns to aggregate, if None (default) all
                        non data/geo are used
            hrr_col: hrr column to create

        Return
        ---------
            data: copy of dataframe
        """
        warnings.warn(
            "Use the function add_geocode(df, 'fips', 'hrr', ...) instead.",
            DeprecationWarning,
        )

        data = self.convert_fips_to_zip(
            data,
            fips_col=fips_col,
            zip_col="zip",
        )
        data = self.convert_zip_to_hrr(
            data,
            zip_col="zip",
            hrr_col=hrr_col,
        )
        data.drop(columns="zip")
        data = data.groupby([fips_col, hrr_col]).sum().reset_index()
        return data

    def fips_to_hrr(
        self, data, fips_col="fips", date_col="date", count_cols=None, hrr_col="hrr"
    ):
        """DEPRECATED
        convert and aggregate from fips to hrr

        Parameters
        ---------
            data: pd.DataFrame input data
            fips_col: fips (county) column to convert
            date_col: date column (is not aggregated)
            count_cols: the count data columns to aggregate, if None (default) all
                        non data/geo are used
            hrr_col: hrr column to create

        Return
        ---------
            data: copy of dataframe
        """
        warnings.warn(
            "Use the function replace_geocode(df, 'fips', 'hrr', ...) instead.",
            DeprecationWarning,
        )

        zip_col = "_zip_col_temp"
        data = self.fips_to_zip(
            data,
            fips_col=fips_col,
            zip_col=zip_col,
            date_col=date_col,
            count_cols=count_cols,
        )
        data = self.zip_to_hrr(
            data,
            zip_col=zip_col,
            date_col=date_col,
            count_cols=count_cols,
            hrr_col=hrr_col,
        )
        return data
