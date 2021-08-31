"""Contains geographic mapping tools.

Authors: Dmitry Shemetov @dshemetov, James Sharpnack @jsharpna, Maria Jahja
Created: 2020-06-01

TODO:
- use a caching utility to store the crossfiles
  see: https://github.com/cmu-delphi/covidcast-indicators/issues/282
"""
# pylint: disable=too-many-lines
from os.path import join

import pandas as pd
import pkg_resources
from pandas.api.types import is_string_dtype

DATA_PATH = "data"
CROSSWALK_FILEPATHS = {
    "zip": {
        "fips": join(DATA_PATH, "zip_fips_table.csv"),
        "hrr": join(DATA_PATH, "zip_hrr_table.csv"),
        "msa": join(DATA_PATH, "zip_msa_table.csv"),
        "pop": join(DATA_PATH, "zip_pop.csv"),
        "state": join(DATA_PATH, "zip_state_code_table.csv"),
        "hhs": join(DATA_PATH, "zip_hhs_table.csv")
    },
    "fips": {
        "zip": join(DATA_PATH, "fips_zip_table.csv"),
        "hrr": join(DATA_PATH, "fips_hrr_table.csv"),
        "msa": join(DATA_PATH, "fips_msa_table.csv"),
        "pop": join(DATA_PATH, "fips_pop.csv"),
        "state": join(DATA_PATH, "fips_state_table.csv"),
        "hhs": join(DATA_PATH, "fips_hhs_table.csv"),
    },
    "state": {"state": join(DATA_PATH, "state_codes_table.csv")},
    "state_code": {
        "hhs": join(DATA_PATH, "state_code_hhs_table.csv"),
        "pop": join(DATA_PATH, "state_pop.csv")
    },
    "state_id": {
        "pop": join(DATA_PATH, "state_pop.csv")
    },
    "state_name": {
        "pop": join(DATA_PATH, "state_pop.csv")
    },
    "jhu_uid": {"fips": join(DATA_PATH, "jhu_uid_fips_table.csv")},
    "hhs": {"pop": join(DATA_PATH, "hhs_pop.csv"),},
    "nation": {"pop": join(DATA_PATH, "nation_pop.csv"),},
}


class GeoMapper:  # pylint: disable=too-many-public-methods
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
    - [x] zip -> hhs
    - [x] zip -> population
    - [x] state code -> hhs
    - [x] fips -> state : unweighted
    - [x] fips -> msa : unweighted
    - [x] fips -> megacounty
    - [x] fips -> hrr
    - [x] fips -> hhs
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

    When replacing geocodes with a new one an aggregation step is performed on the data columns
    to merge entries  (i.e. in the case of a many to one mapping or a weighted mapping). This
    requires a specification of the data columns, which are assumed to be all the columns that
    are not the geocodes or the date column specified in date_col.

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
        """Initialize geomapper.

        Holds loading the crosswalk tables until a conversion function is first used.

        Parameters
        ---------
        crosswalk_files : dict
            A dictionary of the filenames for the crosswalk tables.
        """
        self.crosswalk_filepaths = CROSSWALK_FILEPATHS
        self.crosswalks = {
            "zip": {
                geo: None for geo in ["fips", "hrr", "msa", "pop", "state", "hhs"]
            },
            "fips": {
                geo: None for geo in ["zip", "hrr", "msa", "pop", "state", "hhs"]
            },
            "state": {"state": None},
            "state_code": {"hhs": None, "pop": None},
            "state_id": {"pop": None},
            "state_name": {"pop": None},
            "hhs": {"pop": None},
            "nation": {"pop": None},
            "jhu_uid": {"fips": None},
        }
        self.geo_lists = {
            geo: None for geo in ["zip", "fips", "hrr", "state_id", "state_code",
                                  "state_name", "hhs", "msa"]
        }
        self.geo_lists["nation"] = {"us"}

    # Utility functions
    def _load_crosswalk(self, from_code, to_code):
        """Load the crosswalk from from_code -> to_code."""
        assert from_code in self.crosswalk_filepaths, \
            f"No crosswalk files for {from_code}; try {'; '.join(self.crosswalk_filepaths.keys())}"
        assert to_code in self.crosswalk_filepaths[from_code], \
            f"No crosswalk file from {from_code} to {to_code}; try " \
            f"{'; '.join(self.crosswalk_filepaths[from_code].keys())}"

        if self.crosswalks[from_code][to_code] is None:
            self.crosswalks[from_code][to_code] = self._load_crosswalk_from_file(from_code, to_code)
        return self.crosswalks[from_code][to_code]

    def _load_crosswalk_from_file(self, from_code, to_code):
        stream = pkg_resources.resource_stream(
            __name__, self.crosswalk_filepaths[from_code][to_code]
        )
        usecols = None
        dtype = None
        # Weighted crosswalks
        if (from_code, to_code) in [
            ("zip", "fips"),
            ("fips", "zip"),
            ("jhu_uid", "fips"),
            ("zip", "msa"),
            ("fips", "hrr"),
            ("zip", "hhs")
        ]:
            dtype = {
                from_code: str,
                to_code: str,
                "weight": float,
            }

        # Unweighted crosswalks
        elif (from_code, to_code) in [
            ("zip", "hrr"),
            ("fips", "msa"),
            ("fips", "hhs"),
            ("state_code", "hhs")
        ]:
            dtype = {from_code: str, to_code: str}

        # Special table of state codes, state IDs, and state names
        elif (from_code, to_code) == ("state", "state"):
            dtype = {
                "state_code": str,
                "state_id": str,
                "state_name": str,
            }
        elif (from_code, to_code) == ("zip", "state"):
            dtype = {
                "zip": str,
                "weight": float,
                "state_code": str,
                "state_id": str,
                "state_name": str,
            }
        elif (from_code, to_code) == ("fips", "state"):
            dtype = {
                    "fips": str,
                    "state_code": str,
                    "state_id": str,
                    "state_name": str,
            }

        # Population tables
        elif to_code == "pop":
            dtype = {
                from_code: str,
                "pop": int,
            }
            usecols = [
                from_code,
                "pop"
            ]
        return pd.read_csv(stream, dtype=dtype, usecols=usecols)

    @staticmethod
    def convert_fips_to_mega(data, fips_col="fips", mega_col="megafips"):
        """Convert fips string to a megafips string."""
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
        """Create megacounty column.

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
        - fips -> state_code, state_id, state_name, zip, msa, hrr, nation, hhs
        - zip -> state_code, state_id, state_name, fips, msa, hrr, nation, hhs
        - jhu_uid -> fips
        - state_x -> state_y (where x and y are in {code, id, name}), nation
        - state_code -> hhs, nation

        Parameters
        ---------
        df: pd.DataFrame
            Input dataframe.
        from_code: {'fips', 'zip', 'jhu_uid', 'state_code', 'state_id', 'state_name'}
            Specifies the geocode type of the data in from_col.
        new_code: {'fips', 'zip', 'state_code', 'state_id', 'state_name', 'hrr', 'msa',
                   'hhs'}
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
        assert from_col != new_col, \
            f"Can't use the same column '{from_col}' for both from_col and to_col"
        state_codes = ["state_code", "state_id", "state_name"]

        if not is_string_dtype(df[from_col]):
            if from_code in ["fips", "zip"]:
                df[from_col] = df[from_col].astype(str).str.zfill(5)
            else:
                df[from_col] = df[from_col].astype(str)

        if new_code == "nation":
            return self._add_nation_geocode(df, from_code, from_col, new_col)

        # state codes are all stored in one table
        if from_code in state_codes and new_code in state_codes:
            crosswalk = self._load_crosswalk(from_code="state", to_code="state")
            crosswalk = crosswalk.rename(
                columns={from_code: from_col, new_code: new_col}
            )
        elif new_code in state_codes:
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
        if new_code in state_codes and not from_code in state_codes:
            state_codes.remove(new_code)
            df.drop(columns=state_codes, inplace=True)
        elif new_code in state_codes and from_code in state_codes:
            state_codes.remove(new_code)
            if from_code in state_codes:
                state_codes.remove(from_code)
            df.drop(columns=state_codes, inplace=True)

        return df

    def _add_nation_geocode(self, df, from_code, from_col, new_col):
        """Add a nation geocode column to a dataframe.

        See `add_geocode()` documentation for argument description.
        """
        valid_from_codes = ["fips", "zip", "state_code", "state_name", "state_id"]
        # Assuming that the passed-in records are all United States data, at the moment
        if from_code in valid_from_codes:
            df[new_col] = df[from_col].apply(lambda x: "us")
            return df

        raise ValueError(
            f"Conversion to the nation level is not supported "
            f"from {from_code}; try {valid_from_codes}"
        )

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
        - state_x -> state_y (where x and y are in {code, id, name}), nation
        - state_code -> hhs, nation

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
                   'hhs'}
            Specifies the geocode type of the data in new_col.
        date_col: str or None, default "date"
            Specify which column contains the date values. Used for value aggregation.
            If None, then the aggregation is done only on geo_id.
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

        if not date_col is None:
            df = df.groupby([date_col, new_col]).sum().reset_index()
        else:
            df = df.groupby([new_col]).sum().reset_index()
        return df

    def add_population_column(self, data, geocode_type, geocode_col=None, dropna=True):
        """
        Append a population column to a dataframe, based on the FIPS or ZIP code.

        If no dataframe is provided, the full crosswalk from geocode to population is returned.

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
        supported_geos = ["fips", "zip", "state_id", "state_name", "state_code", "hhs", "nation"]
        if geocode_type not in supported_geos:
            raise ValueError(
                f"Only {supported_geos} geocodes supported. For other codes, aggregate those."
            )
        pop_df = self._load_crosswalk(from_code=geocode_type, to_code="pop")
        if not is_string_dtype(data[geocode_col]):
            if geocode_type in ["zip", "fips"]:
                data[geocode_col] = data[geocode_col].astype(str).str.zfill(5)
            elif geocode_type in ["state_code"]:
                data[geocode_col] = data[geocode_col].astype(str).str.zfill(2)
            else:
                data[geocode_col] = data[geocode_col].astype(str)
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
        """Convert and aggregate from FIPS to megaFIPS.

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

    def as_mapper_name(self, geo_type, state="state_id"):
        """
        Return the mapper equivalent of a region type.

        Human-readable names like 'county' will return their mapper equivalents ('fips').
        """
        if geo_type == "state":
            return state
        if geo_type == "county":
            return "fips"
        return geo_type
    def get_geo_values(self, geo_type):
        """
        Return a set of all values for a given geography type.

        Uses the same caching paradigm as _load_crosswalks, storing the value from previous calls
        and not re-reading the CSVs if the same geo type is requested multiple times. Does not
        share the same crosswalk cache to keep complexity down.

        Reads the FIPS crosswalk files by default for reference data since those have mappings to
        all other geos. Exceptions are nation, which has no mapping file and is hard-coded as 'us',
        and state, which uses the state codes table since the fips/state mapping doesn't include
        all territories.

        Parameters
        ----------
        geo_type: str
          One of "zip", "fips", "hrr", "state_id", "state_code", "state_name", "hhs", "msa",
          and "nation"

        Returns
        -------
        Set of geo values, all in string format.
        """
        if self.geo_lists[geo_type]:  # pylint: disable=no-else-return
            return self.geo_lists[geo_type]
        else:
            from_code = "fips"
            if geo_type.startswith("state"):
                to_code = from_code = "state"
            elif geo_type == "fips":
                to_code = "pop"
            else:
                to_code = geo_type
            stream = pkg_resources.resource_stream(
                __name__, self.crosswalk_filepaths[from_code][to_code]
            )
            crosswalk = pd.read_csv(stream, dtype=str)
            self.geo_lists[geo_type] = set(crosswalk[geo_type])
            return self.geo_lists[geo_type]

    def get_geos_within(self, container_geocode, contained_geocode_type,container_geocode_type):
        """
        Return all contained regions within same container geocode.

        Given a container geocode (e.g. "ca" for California, a state)
        and an enclosed contained geocode type (e.g. "county").
        return a set of container geocode value of the specified type that
        lie within the specified geo code (e.g. all counties within California)
        Support these 3 types:
            - all states within a nation
            - all counties within a state
            - all states within an hhs region

        Parameters
        ----------
        container_geocode: str of identity of nation/state/hhs
            "fips" for return container_geocode of state
            "state_id" for return container_geocode of nation and hhs
        contained_geocode_type: str
            One of "state","county"
        container_geocode_type: str
            One of "state","nation","hhs"

        Returns
        -------
        Set of geo ids of the specified type that lie within the specified container geocode,
        all in string format.
        """
        if contained_geocode_type not in ("county","state"):
            raise ValueError("contained_geocode_type must be one of state, nation and hhs")
        geo_values=set()
        if contained_geocode_type=="state":
            if container_geocode_type=="nation" and container_geocode=="us":
                crosswalk = self._load_crosswalk_from_file("state", "state")
                geo_values=set(crosswalk["state_id"])
            if container_geocode_type=="hhs":
                crosswalk_hhs = self._load_crosswalk_from_file("fips", "hhs")
                crosswalk_state = self._load_crosswalk_from_file("fips", "state")
                fips_hhs=crosswalk_hhs[crosswalk_hhs["hhs"] == container_geocode]["fips"]
                fips_state_id=crosswalk_state[crosswalk_state["fips"].isin(fips_hhs)]["state_id"]
                geo_values.update(fips_state_id)
        elif contained_geocode_type=="county" and container_geocode_type=="state":
            crosswalk = self._load_crosswalk_from_file("fips", "state")
            geo_values=crosswalk[crosswalk["state_id"]==container_geocode]["fips"]
        else:
            raise ValueError("Do not satisfied the reqirement")
        return geo_values
