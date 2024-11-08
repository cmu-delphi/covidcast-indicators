"""
Store backfill data.

Author: Jingjing Tang
Created: 2022-08-03

"""

import calendar
import glob
import os
import pathlib
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Union

# third party
import pandas as pd
from delphi_utils import GeoMapper

from .config import Config

gmpr = GeoMapper()


def store_backfill_file(claims_filepath, _end_date, backfill_dir, logger):
    """
    Store county level backfill data into backfill_dir.

    Parameter:
        claims_filepath: str
            path to the aggregated claims data
        _end_date: datetime
            The most recent date when the raw data is received
        backfill_dir: str
            specified path to store backfill files.
    """
    backfilldata = pd.read_csv(
        claims_filepath,
        usecols=Config.CLAIMS_DTYPES.keys(),
        dtype=Config.CLAIMS_DTYPES,
        parse_dates=[Config.CLAIMS_DATE_COL],
    )
    backfilldata.rename({"ServiceDate": "time_value",
                         "PatCountyFIPS": "fips",
                         "Denominator": "den",
                         "Covid_like": "num"},
                        axis=1, inplace=True)
    backfilldata = gmpr.add_geocode(backfilldata, from_code="fips", new_code="state_id",
                           from_col="fips", new_col="state_id")
    #Store one year's backfill data
    if _end_date.day == 29 and _end_date.month == 2:
        _start_date = datetime(_end_date.year-1, 2, 28)
    else:
        _start_date = _end_date.replace(year=_end_date.year-1)
    selected_columns = ['time_value', 'fips', 'state_id',
                        'den', 'num']
    backfilldata = backfilldata.loc[(backfilldata["time_value"] >= _start_date)
                                    & (~backfilldata["fips"].isnull()),
                                    selected_columns]
    logger.info("Filtering source data", startdate=_start_date, enddate=_end_date)

    backfilldata["lag"] = [(_end_date - x).days for x in backfilldata["time_value"]]
    backfilldata["time_value"] = backfilldata.time_value.dt.strftime("%Y-%m-%d")
    backfilldata["issue_date"] = datetime.strftime(_end_date, "%Y-%m-%d")

    backfilldata = backfilldata.astype({
        "time_value": "string",
        "issue_date": "string",
        "fips": "string",
        "state_id": "string"
    })

    filename = "claims_hosp_as_of_%s.parquet" % datetime.strftime(_end_date, "%Y%m%d")
    path = f"{backfill_dir}/{filename}"

    # Store intermediate file into the backfill folder
    try:
        backfilldata.to_parquet(path, index=False)
        logger.info("Stored source data in parquet", filename=filename)
    except:  # pylint: disable=W0702
        logger.info("Failed to store source data in parquet")
    return path


def merge_existing_backfill_files(backfill_dir, backfill_file, issue_date, logger):
    """
    Merge existing backfill with the patch data included. This function is specifically run for patching.

    When the indicator fails for some reason or another, there's a gap in the backfill files.
    The patch to fill in the missing dates happens later down the line when the backfill files are already merged.
    This function takes the merged files with the missing date, insert the particular date, and merge back the file.
    Parameters
    ----------
    issue_date : datetime
        The most recent date when the raw data is received
    backfill_dir : str
        specified path to store backfill files.
    backfill_file : str
        specific file add to merged backfill file.
    """
    new_files = glob.glob(backfill_dir + "/claims_hosp_*")

    def get_file_with_date(files) -> Union[pathlib.Path, None]:
        for file_path in files:
            # need to only match files with 6 digits for merged files
            pattern = re.findall(r"_(\d{6,6})\.parquet", file_path)
            if pattern:
                file_month = datetime.strptime(pattern[0], "%Y%m").replace(day=1)
                end_date = (file_month + timedelta(days=32)).replace(day=1)
                if file_month <= issue_date < end_date:
                    return Path(file_path)
        return

    file_path = get_file_with_date(new_files)

    if not file_path:
        logger.info("Issue date has no matching merged files", issue_date=issue_date.strftime("%Y-%m-%d"))
        return

    logger.info(
        "Adding missing date to merged file", issue_date=issue_date, filename=backfill_file, merged_filename=file_path
    )

    # Start to merge files
    file_name = Path(file_path).name
    merge_file = f"{file_path.parent}/{file_name}_after_merge.parquet"

    try:
        shutil.copyfile(file_path, merge_file)
        existing_df = pd.read_parquet(merge_file, engine="pyarrow")
        df = pd.read_parquet(backfill_file, engine="pyarrow")
        merged_df = pd.concat([existing_df, df]).sort_values(["time_value", "fips"])
        merged_df.to_parquet(merge_file, index=False)

    # pylint: disable=W0703:
    except Exception as e:
        logger.info("Failed to merge existing backfill files", issue_date=issue_date.strftime("%Y-%m-%d"), msg=e)
        os.remove(merge_file)
        os.remove(backfill_file)
        return

    os.remove(file_path)
    os.rename(merge_file, file_path)
    return


def merge_backfill_file(backfill_dir, most_recent, logger, test_mode=False):
    """
    Merge a month's source data into one file.

    Parameters
    ----------
    most_recent : datetime
        The most recent date when the raw data is received
    backfill_dir : str
        specified path to store backfill files.
    test_mode: bool
    """
    previous_month = (most_recent.replace(day=1) - timedelta(days=1)).strftime("%Y%m")
    new_files = glob.glob(backfill_dir + f"/claims_hosp_as_of_{previous_month}*")
    if len(new_files) == 0: # if no any daily file is stored
        logger.info("No new files to merge; skipping merging")
        return

    def get_date(file_link):
        # Keep the function here consistent with the backfill path in
        # function `store_backfill_file`
        fn = file_link.split("/")[-1].split(".parquet")[0].split("_")[-1]
        return datetime.strptime(fn, "%Y%m%d")

    date_list = sorted(map(get_date, new_files))
    latest_date = max(date_list)
    num_of_days_in_month = calendar.monthrange(latest_date.year, latest_date.month)[1]
    if len(date_list) < (num_of_days_in_month * 0.8) and most_recent != latest_date + timedelta(days=1):
        logger.info("Not enough days, skipping merging", n_file_days=len(date_list))
        return

    logger.info("Merging files", start_date=date_list[0], end_date=date_list[-1])
    # Start to merge files
    pdList = []
    try:
        for fn in new_files:
            df = pd.read_parquet(fn, engine='pyarrow')
            pdList.append(df)
        merged_file = pd.concat(pdList).sort_values(["time_value", "fips"])
        path = f"{backfill_dir}/claims_hosp_{datetime.strftime(latest_date, '%Y%m')}.parquet"
        merged_file.to_parquet(path, index=False)

    except Exception as e:
        logger.info("Failed to merge backfill files", msg=e)
        return

    # Delete daily files once we have the merged one.
    if not test_mode:
        for fn in new_files:
            os.remove(fn)
    return
