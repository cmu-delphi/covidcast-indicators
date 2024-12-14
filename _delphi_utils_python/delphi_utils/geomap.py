"""Contains geographic mapping tools.

Authors: Dmitry Shemetov @dshemetov, James Sharpnack @jsharpna, Maria Jahja
"""

from collections import defaultdict
from os.path import join
from typing import Iterator, List, Literal, Optional, Set, Union

import importlib_resources
import pandas as pd
from pandas.api.types import is_string_dtype


class GeoMapper:
    """Geo mapping tools commonly used in Delphi.

    The GeoMapper class provides utility functions for translating between different
    geocodes. Supported geocodes:

    - zip:          five characters [0-9] with leading 0's, e.g. "33626"
                    also known as zip5 or zip code
    - fips:         five characters [0-9] with leading 0's, e.g. "12057"
                    the first two digits are the state FIPS code and the last
                    three are the county FIPS code
    - msa:          five characters [0-9] with leading 0's, e.g. "90001"
                    also known as metropolitan statistical area
    - state_code:   two characters [0-9], e.g "06"
    - state_id:     two characters [A-Z], e.g "CA"
    - state_name:   human-readable name, e.g "California"
    - state_*:      we use this below to refer to the three above geocodes in aggregate
    - hrr:          an integer from 1-500, also known as hospital
                    referral region
    - hhs:          an integer from 1-10, also known as health and human services region
                    https://www.hhs.gov/about/agencies/iea/regional-offices/index.html

    Valid mappings:

    From            To              Population Weighted
    zip             fips            Yes
    zip             hrr             No
    zip             msa             Yes
    zip             state_*         Yes
    zip             hhs             Yes
    zip             population      --
    zip             nation          No
    state_*         state_*         No
    state_*         hhs             No
    state_*         population      --
    state_*         nation          No
    fips            state_*         No
    fips            msa             No
    fips            megacounty      No
    fips            hrr             Yes
    fips            hhs             No
    fips            chng-fips       No
    fips            nation          No
    chng-fips       state_*         No

    Crosswalk Tables
    ================

    The GeoMapper instance loads pre-generated crosswalk tables (built by the
    script in `data_proc/geomap/geo_data_proc.py`). If a mapping between codes
    is one to one or many to one, then the table has just two columns. If the
    mapping is one to many, then a weight column is provided, which gives the
    fractional population contribution of a source_geo to the target_geo. The
    weights satisfy the condition that df.groupby(from_code).sum(weight) == 1.0
    for all values of from_code.

    Aggregation
    ===========

    The GeoMapper class provides functions to aggregate data from one geocode
    to another. The aggregation can be a simple one-to-one mapping or a
    weighted aggregation. The weighted aggregation is useful when the data
    being aggregated is a population-weighted quantity, such as visits or
    cases. The aggregation is done by multiplying the data columns by the
    weights and summing over the data columns. Note that the aggregation does
    not adjust the aggregation for missing or NA values in the data columns,
    which is equivalent to a zero-fill.

    Example Usage
    =============
    The main GeoMapper object loads and stores crosswalk dataframes on-demand.

    When replacing geocodes with a new one an aggregation step is performed on
    the data columns to merge entries  (i.e. in the case of a many to one
    mapping or a weighted mapping). This requires a specification of the data
    columns, which are assumed to be all the columns that are not the geocodes
    or the date column specified in date_col.

    Example 1: to add a new column with a new geocode, possibly with weights:
    > gmpr = GeoMapper()
    > df = gmpr.add_geocode(df, "fips", "zip",
                            from_col="fips", new_col="geo_id",
                            date_col="timestamp", dropna=False)

    Example 2: to replace a geocode column with a new one, aggregating the data
    with weights:
    > gmpr = GeoMapper()
    > df = gmpr.replace_geocode(df, "fips", "zip",
                                from_col="fips", new_col="geo_id",
                                date_col="timestamp", dropna=False)
    """

    CROSSWALK_FILENAMES = {
        "zip": {
            "fips": "zip_fips_table.csv",
            "hrr": "zip_hrr_table.csv",
            "msa": "zip_msa_table.csv",
            "pop": "zip_pop.csv",
            "state": "zip_state_code_table.csv",
            "hhs": "zip_hhs_table.csv",
        },
        "fips": {
            "chng-fips": "fips_chng-fips_table.csv",
            "zip": "fips_zip_table.csv",
            "hrr": "fips_hrr_table.csv",
            "msa": "fips_msa_table.csv",
            "pop": "fips_pop.csv",
            "state": "fips_state_table.csv",
            "hhs": "fips_hhs_table.csv",
        },
        "hhs": {"pop": "hhs_pop.csv"},
        "chng-fips": {"state": "chng-fips_state_table.csv"},
        "state": {"state": "state_codes_table.csv"},
        "state_code": {"hhs": "state_code_hhs_table.csv", "pop": "state_pop.csv"},
        "state_id": {"pop": "state_pop.csv"},
        "state_name": {"pop": "state_pop.csv"},
        "nation": {"pop": "nation_pop.csv"},
    }

    def __init__(self, census_year: int = 2020):
        """Initialize geomapper.

        Parameters
        ---------
        census_year: int
            Year of Census population data. 2019 estimates and 2020 full Census supported.
        """
        self._crosswalks = defaultdict(dict)
        self._geo_sets = dict()

        # Include all unique geos from first-level and second-level keys in
        # CROSSWALK_FILENAMES, with a few exceptions
        self._geos = {
            subkey
            for mainkey in self.CROSSWALK_FILENAMES
            for subkey in self.CROSSWALK_FILENAMES[mainkey]
        }.union(set(self.CROSSWALK_FILENAMES.keys())) - {"state", "pop"}

        for from_code, to_codes in self.CROSSWALK_FILENAMES.items():
            for to_code, file_path in to_codes.items():
                self._crosswalks[from_code][to_code] = self._load_crosswalk_from_file(
                    from_code, to_code, join("data", f"{census_year}", file_path)
                )

        for geo_type in self._geos:
            self._geo_sets[geo_type] = self._load_geo_values(geo_type)

    def _load_crosswalk_from_file(
        self, from_code: str, to_code: str, data_path: str
    ) -> pd.DataFrame:
        stream = importlib_resources.files(__name__) / data_path
        dtype = {
            from_code: str,
            to_code: str,
            "pop": int,
            "weight": float,
            **{geo: str for geo in self._geos - set("nation")},
        }
        usecols = [from_code, "pop"] if to_code == "pop" else None
        return pd.read_csv(stream, dtype=dtype, usecols=usecols)

    def _load_geo_values(self, geo_type: str) -> Set[str]:
        if geo_type == "nation":
            return {"us"}

        if geo_type.startswith("state"):
            to_code = from_code = "state"
        elif geo_type == "fips":
            from_code = "fips"
            to_code = "state"
        else:
            from_code = "fips"
            to_code = geo_type

        crosswalk = self._crosswalks[from_code][to_code]
        return set(crosswalk[geo_type])

    @staticmethod
    def convert_fips_to_mega(
        data: pd.DataFrame, fips_col: str = "fips", mega_col: str = "megafips"
    ) -> pd.DataFrame:
        """Convert fips or chng-fips string to a megafips string."""
        data = data.copy()
        data[mega_col] = data[fips_col].astype(str).str.zfill(5)
        data[mega_col] = data[mega_col].str.slice_replace(start=2, stop=5, repl="000")
        return data

    @staticmethod
    def megacounty_creation(
        data: pd.DataFrame,
        thr_count: Union[float, int],
        thr_win_len: int,
        thr_col: str = "visits",
        fips_col: str = "fips",
        date_col: str = "timestamp",
        mega_col: str = "megafips",
    ) -> pd.DataFrame:
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

        def agg_sum_iter(data: pd.DataFrame) -> Iterator[pd.DataFrame]:
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
        self,
        df: pd.DataFrame,
        from_code: str,
        new_code: str,
        from_col: Optional[str] = None,
        new_col: Optional[str] = None,
        dropna: bool = True,
    ):
        """Add a new geocode column to a dataframe.

        See class docstring for supported geocode transformations.

        Parameters
        ---------
        df: pd.DataFrame
            Input dataframe.
        from_code: {'fips', 'chng-fips', 'zip', 'state_code',
                    'state_id', 'state_name'}
            Specifies the geocode type of the data in from_col.
        new_code: {'fips', 'chng-fips', 'zip', 'state_code', 'state_id',
                   'state_name', 'hrr', 'msa', 'hhs'}
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
        assert (
            from_col != new_col
        ), f"Can't use the same column '{from_col}' for both from_col and to_col"
        state_codes = ["state_code", "state_id", "state_name"]

        if not is_string_dtype(df[from_col]):
            if from_code in ["fips", "zip", "chng-fips"]:
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
        if new_code in state_codes and from_code not in state_codes:
            state_codes.remove(new_code)
            df.drop(columns=state_codes, inplace=True)
        elif new_code in state_codes and from_code in state_codes:
            state_codes.remove(new_code)
            if from_code in state_codes:
                state_codes.remove(from_code)
            df.drop(columns=state_codes, inplace=True)

        return df

    def _add_nation_geocode(
        self, df: pd.DataFrame, from_code: str, from_col: str, new_col: str
    ) -> pd.DataFrame:
        """Add a nation geocode column to a dataframe.

        See `add_geocode()` documentation for argument description.
        """
        valid_from_codes = ["fips", "zip", "state_code", "state_name", "state_id"]
        # Assuming that the passed-in records are all United States data, at the moment
        if from_code in valid_from_codes:
            df[new_col] = df[from_col].apply(lambda x: "us")
            return df

        raise ValueError(
            "Conversion to the nation level is not supported "
            f"from {from_code}; try {valid_from_codes}"
        )

    def replace_geocode(
        self,
        df: pd.DataFrame,
        from_code: str,
        new_code: str,
        from_col: Optional[str] = None,
        new_col: Optional[str] = None,
        date_col: Optional[str] = "timestamp",
        data_cols: Optional[List[str]] = None,
        dropna: bool = True,
    ) -> pd.DataFrame:
        """Replace a geocode column in a dataframe.

        See class docstring for supported geocode transformations.

        Parameters
        ---------
        df: pd.DataFrame
            Input dataframe.
        from_col: str
            Name of the column in data to match and remove.
        from_code: {'fips', 'zip', 'state_code', 'state_id', 'state_name'}
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

        if date_col is not None:
            df = df.groupby([date_col, new_col]).sum(numeric_only=True).reset_index()
        else:
            df = df.groupby([new_col]).sum(numeric_only=True).reset_index()
        return df

    def add_population_column(
        self,
        data: pd.DataFrame,
        geocode_type: Literal["fips", "zip"],
        geocode_col: Optional[str] = None,
        dropna: bool = True,
    ) -> pd.DataFrame:
        """
        Append a population column to a dataframe, based on the FIPS or ZIP code.

        If no dataframe is provided, the full crosswalk from geocode to population is returned.

        Parameters
        ---------
        data: pd.DataFrame
            The dataframe with a FIPS code column.
        geocode_type:
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
        supported_geos = [
            "fips",
            "zip",
            "state_id",
            "state_name",
            "state_code",
            "hhs",
            "nation",
        ]
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
        data_with_pop = data.merge(
            pop_df, left_on=geocode_col, right_on=geocode_type, how=merge_type
        ).rename(columns={"pop": "population"})
        return data_with_pop

    @staticmethod
    def fips_to_megacounty(
        data: pd.DataFrame,
        thr_count: Union[float, int],
        thr_win_len: int,
        thr_col: str = "visits",
        fips_col: str = "fips",
        date_col: str = "timestamp",
        mega_col: str = "megafips",
        count_cols=None,
    ) -> pd.DataFrame:
        """Convert and aggregate from FIPS or chng-fips to megaFIPS.

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
        data = data.reset_index().groupby([date_col, mega_col]).sum(numeric_only=True)
        return data.reset_index()

    def as_mapper_name(self, geo_type: str, state: str = "state_id") -> str:
        """
        Return the mapper equivalent of a region type.

        Human-readable names like 'county' will return their mapper equivalents ('fips').
        """
        if geo_type == "state":
            return state
        if geo_type == "county":
            return "fips"
        return geo_type

    def get_crosswalk(self, from_code: str, to_code: str) -> pd.DataFrame:
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
            raise ValueError(
                f'Mapping from "{from_code}" to "{to_code}" not found.'
            ) from e

    def get_geo_values(self, geo_type: str) -> Set[str]:
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

    def get_geos_within(
        self,
        container_geocode: str,
        contained_geocode_type: str,
        container_geocode_type: str,
    ) -> Set[str]:
        """
        Return all contained regions of the given type within the given container geocode.

        Given container_geocode (e.g "ca" for California) of type container_geocode_type
        (e.g "state"), return all (contained_geocode_type)s within container_geocode.

        Supports these 4 combinations:
            - all states within a nation
            - all counties within a state
            - all CHNG counties+county groups within a state
            - all states within an hhs region

        Parameters
        ----------
        container_geocode: str
            Instance of nation/state/hhs to find the sub-regions of
        contained_geocode_type: str
            The subregion type to retrieve. One of "state", "county", "fips", "chng-fips"
        container_geocode_type: str
            The parent region type. One of "state", "nation", "hhs"

        Returns
        -------
        Set of geo code strings of the given type that lie within the given container geocode.
        """
        if contained_geocode_type == "state":
            if container_geocode_type == "nation" and container_geocode == "us":
                crosswalk = self._crosswalks["state"]["state"]
                return set(crosswalk["state_id"])
            if container_geocode_type == "hhs":
                crosswalk_hhs = self._crosswalks["fips"]["hhs"]
                crosswalk_state = self._crosswalks["fips"]["state"]
                fips_hhs = crosswalk_hhs[crosswalk_hhs["hhs"] == container_geocode][
                    "fips"
                ]
                return set(
                    crosswalk_state[crosswalk_state["fips"].isin(fips_hhs)]["state_id"]
                )
        elif (
            contained_geocode_type in ("county", "fips", "chng-fips")
            and container_geocode_type == "state"
        ):
            contained_geocode_type = self.as_mapper_name(contained_geocode_type)
            crosswalk = self._crosswalks[contained_geocode_type]["state"]
            return set(
                crosswalk[crosswalk["state_id"] == container_geocode][
                    contained_geocode_type
                ]
            )
        raise ValueError(
            "(contained_geocode_type, container_geocode_type) was "
            f"({contained_geocode_type}, {container_geocode_type}), but "
            "must be one of (state, nation), (state, hhs), (county, state)"
            ", (fips, state), (chng-fips, state)"
        )

    def aggregate_by_weighted_sum(
        self, df: pd.DataFrame, to_geo: str, sensor_col: str, time_col: str, population_col: str
    ) -> pd.DataFrame:
        """Aggregate sensor, weighted by time-dependent population.

        Note: This function generates its own population weights and excludes
        locations where the data is NA, which is effectively an extrapolation
        assumption to the rest of the geos.  This is in contrast to the
        `replace_geocode` function, which assumes that the weights are already
        present in the data and does not adjust for missing data (see the
        docstring for the GeoMapper class).

        Parameters
        ---------
        df: pd.DataFrame
            Input dataframe, assumed to have a sensor column (e.g. "visits"), a
            to_geo column (e.g. "state"), and a population column (corresponding
            to a from_geo, e.g. "wastewater collection site").
        to_geo: str
            The column name of the geocode to aggregate to.
        sensor_col: str
            The column name of the sensor to aggregate.
        time_col: str
            The column name of the timestamp to aggregate over.
        population_column: str
            The column name of the population to weight the sensor by.

        Returns
        ---------
        agg_df: pd.DataFrame
            A dataframe with the aggregated sensor values, weighted by population.
        """
        # Don't modify the input dataframe
        df = df.copy()
        # Zero-out populations where the sensor is NA
        df["_zeroed_pop"] = df[population_col] * df[sensor_col].abs().notna()
        # Weight the sensor by the population
        df["_weighted_sensor"] = df[sensor_col] * df["_zeroed_pop"]
        agg_df = (
            df.groupby([time_col, to_geo])
            .agg(
            {
                "_zeroed_pop": "sum",
                "_weighted_sensor": lambda x: x.sum(min_count=1),
            }
            ).assign(
                _new_sensor = lambda x: x["_weighted_sensor"] / x["_zeroed_pop"]
            ).reset_index()
            .rename(columns={"_new_sensor": f"weighted_{sensor_col}"})
            .drop(columns=["_zeroed_pop", "_weighted_sensor"])
        )

        return agg_df
