import dask.dataframe as dd
from datetime import datetime
import numpy as np
import pandas as pd
from pathlib import Path

from .config import Config


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
        with open(filename, "w") as outfile:
            outfile.write("geo_id,val,se,direction,sample_size\n")

            for line in single_date_df.itertuples():
                geo_id = line.geo_id
                sensor = 100 * line.val  # report percentages
                se_val = 100 * line.se
                assert not np.isnan(sensor), "sensor value is nan, check pipeline"
                assert sensor < 90, f"strangely high percentage {geo_id, sensor}"
                if not np.isnan(se_val):
                    assert se_val < 5, f"standard error suspiciously high! investigate {geo_id}"

                if se:
                    assert sensor > 0 and se_val > 0, "p=0, std_err=0 invalid"
                    outfile.write(
                        "%s,%f,%s,%s,%s\n" % (geo_id, sensor, se_val, "NA", "NA"))
                else:
                    # for privacy reasons we will not report the standard error
                    outfile.write(
                        "%s,%f,%s,%s,%s\n" % (geo_id, sensor, "NA", "NA", "NA"))
                out_n += 1
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
