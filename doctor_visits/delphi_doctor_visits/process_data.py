import dask.dataframe as dd
from datetime import datetime
import numpy as np
import pandas as pd
from pathlib import Path

from .config import Config

def format_df(df: pd.DataFrame, geo_id: str, se: bool, logger):
    '''

    Parameters
    ----------
    df
    geo_id
    se
    logger

    Returns
    -------

    '''
    # report in percentage
    df['val'] = df['val'] * 100
    df["se"] = df["se"] * 100

    val_isnull = df["val"].isnull()
    df_val_null = df[val_isnull]
    if not df_val_null.empty:
        logger.info("sensor value is nan, check pipeline")
    filtered_df = df[~val_isnull]

    se_too_high = filtered_df['se'] >= 5
    df_se_too_high = filtered_df[se_too_high]
    if len(df_se_too_high.empty) > 0:
        logger.info(f"standard error suspiciously high! investigate {geo_id}")
    filtered_df = filtered_df[~se_too_high]

    sensor_too_high = filtered_df['val'] >= 90
    df_sensor_too_high = filtered_df[sensor_too_high]
    if len(df_sensor_too_high) > 0:
        logger.info(f"standard error suspiciously high! investigate {geo_id}")
    filtered_df = filtered_df[~sensor_too_high]

    if se:
        valid_cond = filtered_df['se'] > 0 & filtered_df['val'] > 0
        invalid_df = filtered_df[~valid_cond]
        if len(invalid_df) > 0:
            logger.info(f"p=0, std_err=0 invalid")
        filtered_df = filtered_df[valid_cond]
    else:
        filtered_df.drop(columns=['se'], inplace=True)



def write_to_csv(output_df: pd.DataFrame, geo_level: str, se:bool, out_name: str, logger, output_path="."):
    """Write sensor values to csv.

    Args:
      output_dict: dictionary containing sensor rates, se, unique dates, and unique geo_id
      geo_level: geographic resolution, one of ["county", "state", "msa", "hrr", "nation", "hhs"]
      se: boolean to write out standard errors, if true, use an obfuscated name
      out_name: name of the output file
      output_path: outfile path to write the csv (default is current directory)
    """
    if se:
        logger.info(f"========= WARNING: WRITING SEs TO {out_name} =========")

    out_n = 0
    for d in set(output_df["date"]):
        filename = "%s/%s_%s_%s.csv" % (output_path,
                                        (d + Config.DAY_SHIFT).strftime("%Y%m%d"),
                                        geo_level,
                                        out_name)
        single_date_df = output_df[output_df["date"] == d]

    logger.debug(f"wrote {out_n} rows for {geo_level}")


def csv_to_df(filepath: str, startdate: datetime, enddate: datetime, dropdate: datetime, logger) -> pd.DataFrame:
    '''
    Reads csv using Dask and filters out based on date range and currently unused column,
    then converts back into pandas dataframe.
    Parameters
    ----------
      filepath: path to the aggregated doctor-visits data
      startdate: first sensor date (YYYY-mm-dd)
      enddate: last sensor date (YYYY-mm-dd)
      dropdate: data drop date (YYYY-mm-dd)

    -------
    '''
    filepath = Path(filepath)
    logger.info(f"Processing {filepath}")

    ddata = dd.read_csv(
        filepath,
        compression="gzip",
        dtype=Config.DTYPES,
        blocksize=None,
    )

    ddata = ddata.dropna()
    # rename inconsistent column names to match config column names
    ddata = ddata.rename(columns=Config.DEVIANT_COLS_MAP)

    ddata = ddata[Config.FILT_COLS]
    ddata[Config.DATE_COL] = dd.to_datetime(ddata[Config.DATE_COL])

    # restrict to training start and end date
    startdate = startdate - Config.DAY_SHIFT

    assert startdate > Config.FIRST_DATA_DATE, "Start date <= first day of data"
    assert startdate < enddate, "Start date >= end date"
    assert enddate <= dropdate, "End date > drop date"

    date_filter = ((ddata[Config.DATE_COL] >= Config.FIRST_DATA_DATE) & (ddata[Config.DATE_COL] < dropdate))

    df = ddata[date_filter].compute()

    # aggregate age groups (so data is unique by service date and FIPS)
    df = df.groupby([Config.DATE_COL, Config.GEO_COL]).sum(numeric_only=True).reset_index()
    assert np.sum(df.duplicated()) == 0, "Duplicates after age group aggregation"
    assert (df[Config.COUNT_COLS] >= 0).all().all(), "Counts must be nonnegative"

    logger.info(f"Done processing {filepath}")
    return df
