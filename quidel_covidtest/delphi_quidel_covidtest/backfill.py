# -*- coding: utf-8 -*-
"""Store backfill data."""
import calendar
import os
import glob
import re
import shutil
from datetime import datetime, timedelta
from typing import Union

import pandas as pd

from delphi_utils import GeoMapper


gmpr = GeoMapper()

def store_backfill_file(df, _end_date, backfill_dir, logger):
    """
    Store county level backfill data into backfill_dir.

    Parameter:
        df: pd.DataFrame
            Pre-process file at ZipCode level
        _end_date: datetime
            The most recent date when the raw data is received
        backfill_dir: str
            specified path to store backfill files.
    """
    backfilldata = df.copy()
    backfilldata = gmpr.replace_geocode(backfilldata, from_code="zip", new_code="fips",
                          from_col="zip", new_col="fips", date_col="timestamp")
    backfilldata = gmpr.add_geocode(backfilldata, from_code="fips", new_code="state_id",
                          from_col="fips", new_col="state_id")
    backfilldata.rename({"timestamp": "time_value",
                         "totalTest_total": "den_total",
                         "positiveTest_total": "num_total",
                        "positiveTest_age_0_4": "num_age_0_4",
                        "totalTest_age_0_4": "den_age_0_4",
                        "positiveTest_age_5_17": "num_age_5_17",
                        "totalTest_age_5_17": "den_age_5_17",
                        "positiveTest_age_18_49": "num_age_18_49",
                        "totalTest_age_18_49": "den_age_18_49",
                        "positiveTest_age_50_64": "num_age_50_64",
                        "totalTest_age_50_64": "den_age_50_64",
                        "positiveTest_age_65plus": "num_age_65plus",
                        "totalTest_age_65plus": "den_age_65plus",
                        "positiveTest_age_0_17": "num_age_0_17",
                        "totalTest_age_0_17": "den_age_0_17"},
                        axis=1, inplace=True)
    #Store one year's backfill data
    if _end_date.day == 29 and _end_date.month == 2:
        _start_date = datetime(_end_date.year-1, 2, 28)
    else:
        _start_date = _end_date.replace(year=_end_date.year-1)
    selected_columns = ['time_value', 'fips', 'state_id',
                        'den_total', 'num_total',
                        'num_age_0_4', 'den_age_0_4',
                        'num_age_5_17', 'den_age_5_17',
                        'num_age_18_49', 'den_age_18_49',
                        'num_age_50_64', 'den_age_50_64',
                        'num_age_65plus', 'den_age_65plus',
                        'num_age_0_17', 'den_age_0_17']
    backfilldata = backfilldata.loc[backfilldata["time_value"] >= _start_date,
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

    filename =  "quidel_covidtest_as_of_%s.parquet"%datetime.strftime(_end_date, "%Y%m%d")
    path = f"{backfill_dir}/{filename}"
    # Store intermediate file into the backfill folder
    try:
        backfilldata.to_parquet(path, index=False)
        logger.info("Stored source data in parquet", filename=filename)
    except Exception: # pylint: disable=W0703
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
    new_files = glob.glob(backfill_dir + "/quidel_covidtest_*")

    def get_file_with_date(files) -> Union[str, None]:
        # pylint: disable=R1716
        for filename in files:
            # need to only match files with 6 digits for merged files
            pattern = re.findall(r"_(\d{6,6})\.parquet", filename)
            if pattern:
                file_month = datetime.strptime(pattern[0], "%Y%m").replace(day=1)
                end_date = (file_month + timedelta(days=32)).replace(day=1)
                if issue_date >= file_month and issue_date < end_date:
                    return filename
        # pylint: enable=R1716
        return ""

    file_name = get_file_with_date(new_files)

    if len(file_name) == 0:
        logger.info("Issue date has no matching merged files", issue_date=issue_date.strftime("%Y-%m-%d"))
        return

    logger.info("Adding missing date to merged file", issue_date=issue_date,
        filename=backfill_file, merged_filename=file_name)

    # Start to merge files
    merge_file = f"{file_name.split('.')[0]}_after_merge.parquet"
    try:
        shutil.copyfile(file_name, merge_file)
        existing_df = pd.read_parquet(merge_file, engine="pyarrow")
        df = pd.read_parquet(backfill_file, engine="pyarrow")
        merged_df = pd.concat([existing_df, df]).sort_values(["time_value", "fips"])
        merged_df.to_parquet(merge_file, index=False)
        os.remove(file_name)
        os.rename(merge_file, file_name)
    except Exception as e: # pylint: disable=W0703
        os.remove(merge_file)
        logger.error(e)
    return

def merge_backfill_file(backfill_dir, today, logger, test_mode=False):
    """
    Merge month's backfill data into one file.

    Parameters
    ----------
    today : datetime
        The most recent date when the raw data is received
    backfill_dir : str
        specified path to store backfill files.
    test_mode: bool
    """
    previous_month = (today.replace(day=1) - timedelta(days=1)).strftime("%Y%m")
    new_files = glob.glob(backfill_dir + f"/quidel_covidtest_as_of_{previous_month}*")
    if len(new_files) == 0: # if no any daily file is stored
        logger.info("No new files to merge; skipping merging")
        return

    def get_date(file_link):
        # Keep the function here consistent with the backfill path in
        # function `store_backfill_file`
        fn = file_link.split("/")[-1].split(".parquet")[0].split("_")[-1]
        return datetime.strptime(fn, "%Y%m%d")

    date_list = list(map(get_date, new_files))
    latest_date = max(date_list)

    # Check whether to merge
    # Check the number of files that are not merged
    date_list = list(map(get_date, new_files))
    num_of_days_in_month = calendar.monthrange(latest_date.year, latest_date.month)[1]
    if len(date_list) < num_of_days_in_month:
        logger.info("Not enough days, skipping merging", n_file_days=len(date_list))
        return

    # Start to merge files
    logger.info("Merging files", start_date=date_list[0], end_date=date_list[-1])
    pdList = []
    for fn in new_files:
        df = pd.read_parquet(fn, engine='pyarrow')
        pdList.append(df)
    merged_file = pd.concat(pdList).sort_values(["time_value", "fips"])
    path = backfill_dir + f"/quidel_covidtest_{datetime.strftime(latest_date, '%Y%m')}.parquet"
    merged_file.to_parquet(path, index=False)

    # Delete daily files once we have the merged one.
    if not test_mode:
        for fn in new_files:
            os.remove(fn)
    return
