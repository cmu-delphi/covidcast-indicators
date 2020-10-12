"""Contains geographic mapping tools."""
from delphi_utils import GeoMapper

date_col = "timestamp"
data_cols = ['totalTest', 'numUniqueDevices', 'positiveTest', "population"]
gmpr = GeoMapper() # Use geo utils
GEO_KEY_DICT = {
        "county": "fips",
        "msa": "msa",
        "hrr": "hrr",
        "state": "state_id"
}
def geo_map(geo_res, df):
    data = df.copy()
    geo_key = GEO_KEY_DICT[geo_res]
    # Add population for each zipcode
    data = gmpr.add_population_column(data, "zip")
    # zip -> geo_res
    data = gmpr.replace_geocode(data, "zip", geo_key,
                                date_col=date_col, data_cols=data_cols)        
    if geo_res == "state":
        return data
    else:
        # Add parent state
        data = add_parent_state(data, geo_res, geo_key)
        return data, geo_key

def add_parent_state(data, geo_res, geo_key):
    """
    - map from msa/hrr to state, going by the state with the largest
      population (since a msa/hrr may span multiple states)
    - map from county to the corresponding state
    """
    fips_to_state = gmpr._load_crosswalk(from_code="fips", to_code="state")
    if geo_res == "county":
        mix_map = fips_to_state[["fips", "state_id"]]
    else:
        fips_to_geo_res = gmpr._load_crosswalk(from_code="fips", to_code=geo_res)    
        mix_map = fips_to_geo_res[["fips", geo_res]].merge(
                fips_to_state[["fips", "state_id"]],
                on="fips",
                how="inner")
        mix_map = gmpr.add_population_column(mix_map, "fips").groupby(
                geo_res).max().reset_index().drop(
                ["fips", "population"], axis = 1)
    # Merge the info of parent state to the data
    data = data.merge(mix_map, how="left", on=geo_key).drop(
        columns=["population"]).dropna()
    data = data.groupby(["timestamp", geo_key, "state_id"]).sum().reset_index()
    return data
