# -*- coding: utf-8 -*-
import pandas as pd


INCIDENCE_BASE = 100000
# https://code.activestate.com/recipes/577775-state-fips-codes-dict/
STATE_TO_FIPS = {
    "WA": "53",
    "DE": "10",
    "DC": "11",
    "WI": "55",
    "WV": "54",
    "HI": "15",
    "FL": "12",
    "WY": "56",
    "PR": "72",
    "NJ": "34",
    "NM": "35",
    "TX": "48",
    "LA": "22",
    "NC": "37",
    "ND": "38",
    "NE": "31",
    "TN": "47",
    "NY": "36",
    "PA": "42",
    "AK": "02",
    "NV": "32",
    "NH": "33",
    "VA": "51",
    "CO": "08",
    "CA": "06",
    "AL": "01",
    "AR": "05",
    "VT": "50",
    "IL": "17",
    "GA": "13",
    "IN": "18",
    "IA": "19",
    "MA": "25",
    "AZ": "04",
    "ID": "16",
    "CT": "09",
    "ME": "23",
    "MD": "24",
    "OK": "40",
    "OH": "39",
    "UT": "49",
    "MO": "29",
    "MN": "27",
    "MI": "26",
    "RI": "44",
    "KS": "20",
    "MT": "30",
    "MS": "28",
    "SC": "45",
    "KY": "21",
    "OR": "41",
    "SD": "46",
}

SECONDARY_FIPS = [
    ("51620", ["51093", "51175"]),
    ("51685", ["51153"]),
    ("28039", ["28059", "28041", "28131", "28045", "28059", "28109", "28047"]),
    ("51690", ["51089", "51067"]),
    ("51595", ["51081", "51025", "51175", "51183"]),
    ("51600", ["51059", "51059", "51059"]),
    ("51580", ["51005"]),
    ("51678", ["51163"]),
]
REPLACE_FIPS = [
    ("02158", "02270"),
    ("46102", "46113"),
]

FIPS_TO_STATE = {v: k.lower() for k, v in STATE_TO_FIPS.items()}

def fips_to_state(fips: str) -> str:
    """Wrapper that handles exceptions to the FIPS scheme in the CDS data.

    The two known exceptions to the FIPS encoding are documented in the CDS
    case data README.  All other county FIPS codes are mapped to state by
    taking the first two digits of the five digit, zero-padded county FIPS
    and applying FIPS_TO_STATE to map it to the two-letter postal
    abbreviation.

    Parameters
    ----------
    fips: str
        Five digit, zero padded county FIPS code

    Returns
    -------
    str
        Two-letter postal abbreviation, lower case.

    Raises
    ------
    KeyError
        Inputted FIPS code not recognized.
    """
    return FIPS_TO_STATE[fips[:2]]


def disburse(df: pd.DataFrame, pooled_fips: str, fips_list: list):
    """Disburse counts from POOLED_FIPS equally to the counties in FIPS_LIST.

    Parameters
    ----------
    df: pd.DataFrame
        Columns: fips, timestamp, new_counts, cumulative_counts, ...
    pooled_fips: str
        FIPS of county from which to disburse counts
    fips_list: list[str]
        FIPS of counties to which to disburse counts.

    Results
    -------
    pd.DataFrame
        Dataframe with same schema as df, with the counts disbursed.
    """
    COLS = ["new_counts", "cumulative_counts"]
    df = df.copy().sort_values(["fips", "timestamp"])
    for col in COLS:
        # Get values from the aggregated county:
        vals = df.loc[df["fips"] == pooled_fips, col].values / len(fips_list)
        for fips in fips_list:
            df.loc[df["fips"] == fips, col] += vals
    return df


def geo_map(df: pd.DataFrame, geo_res: str, map_df: pd.DataFrame):
    """
    Maps a DataFrame df, which contains data at the county resolution, and
    aggregate it to the geographic resolution geo_res.

    Parameters
    ----------
    df: pd.DataFrame
        Columns: fips, timestamp, new_counts, cumulative_counts, population ...
    geo_res: str
        Geographic resolution to which to aggregate.  Valid options:
        ('county', 'state', 'msa', 'hrr').
    map_df: pd.DataFrame
        Loaded from static file "fips_prop_pop.csv".

    Returns
    -------
    pd.DataFrame
        Columns: geo_id, timestamp, ...
    """
    VALID_GEO_RES = ("county", "state", "msa", "hrr")
    if geo_res not in VALID_GEO_RES:
        raise ValueError(f"geo_res must be one of {VALID_GEO_RES}")

    df = df.copy()

    if geo_res == "county":
        df["geo_id"] = df["fips"]
    elif geo_res == "state":
        # Grab first two digits of fips
        # Map state fips to us postal code
        df["geo_id"] = df["fips"].apply(fips_to_state)
    elif geo_res in ("msa", "hrr"):
        # Map "missing" secondary FIPS to those that are in our canonical set
        for fips, fips_list in SECONDARY_FIPS:
            df = disburse(df, fips, fips_list)
        for cds_fips, our_fips in REPLACE_FIPS:
            df.loc[df["fips"] == cds_fips, "fips"] = our_fips
        colname = "cbsa_id" if geo_res == "msa" else "hrrnum"
        map_df = map_df.loc[~pd.isnull(map_df[colname])].copy()
        map_df["geo_id"] = map_df[colname].astype(int)
        df["fips"] = df["fips"].astype(int)
        merged = pd.merge(df, map_df, on="fips")
        merged["cumulative_counts"] = merged["cumulative_counts"] * merged["pop_prop"]
        merged["new_counts"] = merged["new_counts"] * merged["pop_prop"]
        merged["population"] = merged["population"] * merged["pop_prop"]
        df = merged.drop(["zip", "pop_prop", "hrrnum", "cbsa_id"], axis=1)
    df = df.drop("fips", axis=1)
    df = df.groupby(["geo_id", "timestamp"]).sum().reset_index()

    # Value would be negative for megacounties , which would not be considered in the main function
    df["incidence"] = df["new_counts"] / df["population"] * INCIDENCE_BASE
    df["cumulative_prop"] = df["cumulative_counts"] / df["population"] * INCIDENCE_BASE
    return df
