"""Contains geographic mapping tools."""

def geo_map(geo_res, data, map_df):"""Contains geographic mapping tools."""
from delphi_utils import GeoMapper

DATE_COL = "timestamp"
DATA_COLS = ['totalTest', 'numUniqueDevices', 'positiveTest', "population"]
GMPR = GeoMapper()  # Use geo utils
GEO_KEY_DICT = {
        "county": "fips",
        "msa": "msa",
        "hrr": "hrr",
        "state": "state_id"
}


def geo_map(geo_res, df):
    """Map a geocode to a new value."""
    data = df.copy()
    geo_key = GEO_KEY_DICT[geo_res]
    # Add population for each zipcode
    data = GMPR.add_population_column(data, "zip")
    # zip -> geo_res
    data = GMPR.replace_geocode(data, "zip", geo_key,
                                date_col=DATE_COL, data_cols=DATA_COLS)
    if geo_res == "state":
        return data
    # Add parent state
    data = add_parent_state(data, geo_res, geo_key)
    return data, geo_key


def add_parent_state(data, geo_res, geo_key):
    """
    Add parent state column to DataFrame.

    - map from msa/hrr to state, going by the state with the largest
      population (since a msa/hrr may span multiple states)
    - map from county to the corresponding state
    """
    fips_to_state = GMPR._load_crosswalk(from_code="fips", to_code="state")  # pylint: disable=protected-access
    if geo_res == "county":
        mix_map = fips_to_state[["fips", "state_id"]]  # pylint: disable=unsubscriptable-object
    else:
        fips_to_geo_res = GMPR._load_crosswalk(from_code="fips", to_code=geo_res)  # pylint: disable=protected-access
        mix_map = fips_to_geo_res[["fips", geo_res]].merge(
                fips_to_state[["fips", "state_id"]],  # pylint: disable=unsubscriptable-object
                on="fips",
                how="inner")
        mix_map = GMPR.add_population_column(mix_map, "fips").groupby(
                geo_res).max().reset_index().drop(
                ["fips", "population"], axis = 1)
    # Merge the info of parent state to the data
    data = data.merge(mix_map, how="left", on=geo_key).drop(
        columns=["population"]).dropna()
    data = data.groupby(["timestamp", geo_key, "state_id"]).sum().reset_index()
    return data

    """Call appropriate mapping function based on desired geo resolution."""
    if geo_res == "county":
        return zip_to_county(data, map_df)
    if geo_res == "msa":
        return zip_to_msa(data, map_df)
    if geo_res == "hrr":
        return zip_to_hrr(data, map_df)
    return zip_to_state(data, map_df)

def zip_to_msa(data, map_df):
    """Map from zipcode to MSA (along with parent state).

    Args:
        data: dataframe at the day-zip resolution.
    Returns:
        tuple, a dataframe at day-msa, with parent state column, and their string keys
    """
    # zip -> msa
    zip_map = map_df[["zip", "cbsa_id"]].dropna().drop_duplicates()
    # forget about the rest of the zips that aren't in MSA
    data = data.merge(zip_map, how="left", on="zip").dropna().drop(columns=["zip"], axis=1)

    # msa + parent state
    # msa_map has mapping from msa to state, going by the state with the largest
    # population (since a msa may span multiple states)
    msa_map = map_df[["cbsa_id", "state_id", "population"]]
    msa_map = msa_map.groupby(["cbsa_id"]).max().reset_index()
    data = data.merge(msa_map, how="left", on="cbsa_id").drop(
        columns=["population"]).dropna()
    data = data.groupby(["timestamp", "cbsa_id", "state_id"]).sum().reset_index()
    data["cbsa_id"] = data["cbsa_id"].apply(lambda x: str(int(x)).zfill(5))

    return data, "cbsa_id"

def zip_to_hrr(data, map_df):
    """Map from zipcode to HRR (along with parent state).

    Args:
        data: dataframe at the day-zip resolution.
    Returns:
        tuple, a dataframe at day-msa, with parent state column, and their string keys
    """
    # zip -> msa
    zip_map = map_df[["zip", "hrrnum"]].dropna().drop_duplicates()
    # forget about the rest of the zips that aren't in MSA
    data = data.merge(zip_map, how="left", on="zip").dropna().drop(columns=["zip"], axis=1)

    # msa + parent state
    # msa_map has mapping from msa to state, going by the state with the largest
    # population (since a msa may span multiple states)
    msa_map = map_df[["hrrnum", "state_id", "population"]]
    msa_map = msa_map.groupby(["hrrnum"]).max().reset_index()
    data = data.merge(msa_map, how="left", on="hrrnum").drop(
        columns=["population"]).dropna()
    data = data.groupby(["timestamp", "hrrnum", "state_id"]).sum().reset_index()
    data["hrrnum"] = data["hrrnum"].astype(int)

    return data, "hrrnum"

def zip_to_county(data, map_df):
    """Aggregate zip codes to the county resolution, along with its parent state.

    Args:
        data: dataframe aggregated to the day-zip resolution
    Returns:
        dataframe at the day-county resolution and parent state, with their string keys
    """
    # zip -> county + parent state (county has unique state)
    zip_map = map_df[["fips", "zip", "state_id"]].dropna().drop_duplicates()
    data = data.merge(zip_map, how="left", on="zip").drop(columns=["zip"]).dropna()
    data = data.groupby(["timestamp", "fips", "state_id"]).sum().reset_index()
    data["fips"] = data["fips"].apply(lambda x: str(int(x)).zfill(5))

    return data, "fips"

def zip_to_state(data, map_df):
    """Aggregate zip codes to the state resolution.

    Args:
        data: dataframe aggregated to the day-zip resolution
    Returns:
        dataframe at the day-state resolution, with the state key
    """
    zip_map = map_df[["zip", "state_id"]].dropna().drop_duplicates()
    data = data.merge(zip_map, how="left", on="zip").drop(
        columns=["zip"]).dropna()
    data = data.groupby(["timestamp", "state_id"]).sum().reset_index()
    return data
