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

    CROSSWALK_FILENAMES = {
        "zip": {
            "fips": "zip_fips_table.csv",
            "hrr": "zip_hrr_table.csv",
            "msa": "zip_msa_table.csv",
            "pop": "zip_pop.csv",
            "state": "zip_state_code_table.csv",
            "hhs": "zip_hhs_table.csv"
        },
        "fips": {
            "zip": "fips_zip_table.csv",
            "hrr": "fips_hrr_table.csv",
            "msa": "fips_msa_table.csv",
            "pop": "fips_pop.csv",
            "state": "fips_state_table.csv",
            "hhs": "fips_hhs_table.csv",
        },
        "state": {"state": "state_codes_table.csv"},
        "state_code": {
            "hhs": "state_code_hhs_table.csv",
            "pop": "state_pop.csv"
        },
        "state_id": {
            "pop": "state_pop.csv"
        },
        "state_name": {
            "pop": "state_pop.csv"
        },
        "jhu_uid": {"fips": "jhu_uid_fips_table.csv"},
        "hhs": {"pop": "hhs_pop.csv"},
        "nation": {"pop": "nation_pop.csv"},
    }

    def __init__(self, census_year=2020):
        """Initialize geomapper.

        Parameters
        ---------
        census_year: int
            Year of Census population data. 2019 estimates and 2020 full Census supported.
        """
        self._crosswalks = {
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
        self._geo_sets = {
            geo: None for geo in ["zip", "fips", "hrr", "state_id", "state_code",
                                  "state_name", "hhs", "msa", "nation"]
        }

        for from_code, to_codes in self.CROSSWALK_FILENAMES.items():
            for to_code, file_path in to_codes.items():
                self._crosswalks[from_code][to_code] = \
                    self._load_crosswalk_from_file(from_code,
                                                   to_code,
                                                   join(f"data/{census_year}", file_path)
                                                   )

        for geo_type in self._geo_sets:
            self._geo_sets[geo_type] = self._load_geo_values(geo_type)

    def _load_crosswalk_from_file(self, from_code, to_code, data_path):
        stream = pkg_resources.resource_stream(__name__, data_path)
        dtype = {
            from_code: str,
            to_code: str,
            "fips": str,
            "zip": str,
            "hrr": str,
            "hhs": str,
            "msa": str,
            "state_code": str,
            "state_id": str,
            "state_name": str,
            "pop": int,
            "weight": float
        }
        usecols = [from_code, "pop"] if to_code == "pop" else None
        return pd.read_csv(stream, dtype=dtype, usecols=usecols)

    def _load_geo_values(self, geo_type):
        if geo_type == "nation":
            return {"us"}

        if geo_type.startswith("state"):
            to_code = from_code = "state"
        elif geo_type == "fips":
            from_code = "fips"
            to_code = "pop"
        else:
            from_code = "fips"
            to_code = geo_type

        crosswalk = self._crosswalks[from_code][to_code]
        return set(crosswalk[geo_type])

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
        date_col="timestamp",
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
            crosswalk = self._crosswalks["state"]["state"]
            crosswalk = crosswalk.rename(
                columns={from_code: from_col, new_code: new_col}
            )
        elif new_code in state_codes:
            crosswalk = self._crosswalks[from_code]["state"]
            crosswalk = crosswalk.rename(
                columns={from_code: from_col, new_code: new_col}
            )
        else:
            crosswalk = self._crosswalks[from_code][new_code]
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
        date_col="timestamp",
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
        date_col: str or None, default "timestamp"
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
        dropna: bool, default True
            Determine whether to drop rows with no population data.

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
        pop_df = self._crosswalks[geocode_type]["pop"]
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
        date_col="timestamp",
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

    def get_crosswalk(self, from_code, to_code):
        """Return a dataframe mapping the given geocodes.

        Parameters
        ----------
        from_code: str
          The geo type to translate from.
        to_code: str
          The geo type (or "pop" for population) to translate to.

        Returns
        -------
        A dataframe containing columbs with the two specified geo types.
        """
        try:
            return self._crosswalks[from_code][to_code]
        except KeyError as e:
            raise ValueError(f'Mapping from "{from_code}" to "{to_code}" not found.') from e

    def get_geo_values(self, geo_type):
        """
        Return a set of all values for a given geography type.

        Parameters
        ----------
        geo_type: str
          One of "zip", "fips", "hrr", "state_id", "state_code", "state_name", "hhs", "msa",
          and "nation"

        Returns
        -------
        Set of geo values, all in string format.
        """
        try:
            return self._geo_sets[geo_type]
        except KeyError as e:
            raise ValueError(f'Given geo type "{geo_type}" not found') from e

    def get_geos_within(self, container_geocode, contained_geocode_type, container_geocode_type):
        """
        Return all contained regions of the given type within the given container geocode.

        Given container_geocode (e.g "ca" for California) of type container_geocode_type
        (e.g "state"), return:
            - all (contained_geocode_type)s within container_geocode

        Supports these 3 combinations:
            - all states within a nation
            - all counties within a state
            - all states within an hhs region

        Parameters
        ----------
        container_geocode: str
            Instance of nation/state/hhs to find the sub-regions of
        contained_geocode_type: str
            The subregion type to retrieve. One of "state", "county"
        container_geocode_type: str
            The parent region type. One of "state", "nation", "hhs"

        Returns
        -------
        Set of geo code strings of the given type that lie within the given container geocode.
        """
        if contained_geocode_type == "state":
            if container_geocode_type == "nation" and container_geocode == "us":
                crosswalk = self._crosswalks["state"]["state"]
                return set(crosswalk["state_id"])   # pylint: disable=unsubscriptable-object
            if container_geocode_type == "hhs":
                crosswalk_hhs = self._crosswalks["fips"]["hhs"]
                crosswalk_state = self._crosswalks["fips"]["state"]
                fips_hhs = crosswalk_hhs[crosswalk_hhs["hhs"] == container_geocode]["fips"]
                return set(crosswalk_state[crosswalk_state["fips"].isin(fips_hhs)]["state_id"])
        elif contained_geocode_type == "county" and container_geocode_type == "state":
            crosswalk = self._crosswalks["fips"]["state"]
            return set(crosswalk[crosswalk["state_id"] == container_geocode]["fips"])
        raise ValueError("(contained_geocode_type, container_geocode_type) was "
                         f"({contained_geocode_type}, {container_geocode_type}), but "
                         f"must be one of (state, nation), (state, hhs), (county, state)")
