import dask.dataframe as dd
from datetime import datetime
import numpy as np
import pandas as pd
from pathlib import Path

from .config import Config

def format_outname(prefix: str, se: bool, weekday:bool):
    '''

    Parameters
    ----------
    prefix
    se
    weekday

    Returns
    -------

    '''
    # write out results
    out_name = "smoothed_adj_cli" if weekday else "smoothed_cli"
    if se:
        assert prefix is not None, "template has no obfuscated prefix"
        out_name = prefix + "_" + out_name
    return out_name

def format_df(df: pd.DataFrame, geo_id: str, se: bool, logger):
    '''
    format dataframe and checks for anomalies to write results
    Parameters
    ----------
    df: dataframe from output from update_sensor
    geo_id: geographic resolution, one of ["county", "state", "msa", "hrr", "nation", "hhs"]
    se: boolean to write out standard errors, if true, use an obfuscated name
    logger

    Returns
    -------
    filtered and formatted dataframe
    '''
    # report in percentage
    df['val'] = df['val'] * 100
    df["se"] = df["se"] * 100

    val_isnull = df["val"].isnull()
    df_val_null = df[val_isnull]
    if not df_val_null.empty:
        logger.info("sensor value is nan, check pipeline")
    df = df[~val_isnull]

    se_too_high = df['se'] >= 5
    df_se_too_high = df[se_too_high]
    if len(df_se_too_high) > 0:
        logger.info(f"standard error suspiciously high! investigate {geo_id}")
    df = df[~se_too_high]

    sensor_too_high = df['val'] >= 90
    df_sensor_too_high = df[sensor_too_high]
    if len(df_sensor_too_high) > 0:
        logger.info(f"standard error suspiciously high! investigate {geo_id}")
    df = df[~sensor_too_high]

    if se:
        valid_cond = (df['se'] > 0) & (df['val'] > 0)
        invalid_df = df[~valid_cond]
        if len(invalid_df) > 0:
            logger.info(f"p=0, std_err=0 invalid")
        df = df[valid_cond]
    else:
        df["se"] = np.NAN

    df["direction"] = np.NAN
    df["sample_size"] = np.NAN
    return df

def write_to_csv(output_df: pd.DataFrame, prefix: str, geo_id: str, weekday: bool, se:bool, logger, output_path="."):
    """Write sensor values to csv.

    Args:
      output_dict: dictionary containing sensor rates, se, unique dates, and unique geo_id
      geo_id: geographic resolution, one of ["county", "state", "msa", "hrr", "nation", "hhs"]
      se: boolean to write out standard errors, if true, use an obfuscated name
      out_name: name of the output file
      output_path: outfile path to write the csv (default is current directory)
    """
    out_name = format_outname(prefix, se, weekday)
    filtered_df = format_df(output_df, geo_id, se, logger)

    if se:
        logger.info(f"========= WARNING: WRITING SEs TO {out_name} =========")

    dates = set(list(output_df['date']))
    grouped = filtered_df.groupby('date')
    for d in dates:
        filename = "%s/%s_%s_%s.csv" % (output_path,
                                        (d + Config.DAY_SHIFT).strftime("%Y%m%d"),
                                        geo_id,
                                        out_name)
        single_date_df = grouped.get_group(d)
        single_date_df = single_date_df.drop(columns=['date'])
        single_date_df.to_csv(filename, index=False, na_rep="NA")

        logger.debug(f"wrote {len(single_date_df)} rows for {geo_id}")


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
