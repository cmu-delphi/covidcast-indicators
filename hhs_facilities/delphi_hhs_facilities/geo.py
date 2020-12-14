"""Functions for mapping geographic regions."""

import pandas as pd
from numpy import dtype
from delphi_utils.geomap import GeoMapper


def convert_geo(df: pd.DataFrame, geo: str, gmpr: GeoMapper) -> pd.DataFrame:
    """
    Map a DataFrame to desired regions.

    The HHS facility level data contains columns for zip, state, and fips. For state and fips, we
    use them as given. For all other geos, we map from zip (the smallest of the regions) to the
    desired geo.

    Parameters
    ----------
    df: pd.DataFrame
        Input DataFrame containing zip, state, and fips columns.
    geo:
        Desired new geographic resolution.
    gmpr:
        GeoMapper object.

    Returns
    -------
    DataFrame containing new geography column `geo_id` in the `geo` resolution.
    """
    if geo == "county":
        output_df = df.copy()
        output_df["geo_id"] = output_df["fips_code"]
    elif geo == "state":
        output_df = df.copy()
        output_df["geo_id"] = output_df["state"]
    elif geo == "hrr":  # use zip for HRR since zips nest within HRR while FIPS split across HRRs.
        output_df = gmpr.add_geocode(df, "zip", geo)
        output_df["geo_id"] = output_df[geo]
    else:
        output_df = gmpr.add_geocode(df, "fips", geo, from_col="fips_code")
        output_df["geo_id"] = output_df[geo]
    return output_df


def fill_missing_fips(df: pd.DataFrame, gmpr: GeoMapper) -> pd.DataFrame:
    """
    Fill in missing FIPS code if zip is present.

    Maps rows that have the FIPS missing but zip present. The rest of the rows,
    including those where both FIPS and zip are nan, are kept as is and appended back at the end.
    Rows with a zip which fail to map to a FIPS are also kept so that column totals remain equal.
    This means that column sums before and after imputation should be identical, and any dropping
    of values is handled by downstream geomapping.

    TODO #636 Generalize this function to geomapper.

    Parameters
    ----------
    df: pd.DataFrame
        Input DataFrame containing zip and fips columns.
    gmpr:
        GeoMapper object.

    Returns
    -------
    DataFrame with missing FIPS imputed with zip.
    """
    mask = pd.isna(df["fips_code"]) & ~pd.isna(df["zip"])
    no_fips = df[mask]
    fips_present = df[~mask]
    no_data_cols = [c for c in df.columns if df[c].dtypes not in (dtype("int64"), dtype("float64"))]
    data_cols = list(set(df.columns) - set(no_data_cols))
    added_fips = gmpr.add_geocode(no_fips, "zip", "fips", dropna=False)
    added_fips["fips_code"] = added_fips["fips"]
    # set weight of unmapped zips to 1 to they don't zero out all the values when multiplied
    added_fips.weight.fillna(1, inplace=True)
    added_fips[data_cols] = added_fips[data_cols].multiply(added_fips["weight"], axis=0)
    fips_filled = added_fips.groupby(no_data_cols, dropna=False, as_index=False).sum(min_count=1)
    fips_filled.drop(columns="weight", inplace=True)
    return pd.concat([fips_present, fips_filled]).reset_index(drop=True)
