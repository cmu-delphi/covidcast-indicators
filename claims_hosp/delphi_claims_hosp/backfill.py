"""
Store backfill data.

Author: Jingjing Tang
Created: 2022-08-03

"""

import glob
import os
import pathlib
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple, Union

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

    backfilldata["lag"] = [(_end_date - x).days for x in backfilldata["time_value"]]
    backfilldata["time_value"] = backfilldata.time_value.dt.strftime("%Y-%m-%d")
    backfilldata["issue_date"] = datetime.strftime(_end_date, "%Y-%m-%d")

    backfilldata = backfilldata.astype({
        "time_value": "string",
        "issue_date": "string",
        "fips": "string",
        "state_id": "string"
    })

    filename = backfill_dir + "/claims_hosp_as_of_%s.parquet" % datetime.strftime(_end_date, "%Y%m%d")
    # Store intermediate file into the backfill folder
    backfilldata.to_parquet(filename, index=False)

    # Store intermediate file into the backfill folder
    try:
        backfilldata.to_parquet(filename, index=False)
        logger.info("Stored source data in parquet", filename=filename)
    except:  # pylint: disable=W0702
        logger.info("Failed to store source data in parquet")
    return filename


def merge_backfill_file(backfill_dir, backfill_merge_day, today, logger, test_mode=False, check_nd=25):
    """
    Merge ~4 weeks' backfill data into one file.

    Usually this function should merge 28 days' data into a new file so as to
    save the reading time when running the backfill pipelines. We set a softer
    threshold to allow flexibility in data delivery.
    Parameters
    ----------
    today : datetime
        The most recent date when the raw data is received
    backfill_dir : str
        specified path to store backfill files.
    backfill_merge_day: int
        The day of a week that we used to merge the backfill files. e.g. 0
        is Monday.
    test_mode: bool
    check_nd: int
        The criteria of the number of unmerged files. Ideally, we want the
        number to be 28, but we use a looser criteria from practical
        considerations
    """
    new_files = glob.glob(backfill_dir + "/claims_hosp_as_of_*")
    if len(new_files) == 0: # if no any daily file is stored
        return

    def get_date(file_link):
        # Keep the function here consistent with the backfill path in
        # function `store_backfill_file`
        fn = file_link.split("/")[-1].split(".parquet")[0].split("_")[-1]
        return datetime.strptime(fn, "%Y%m%d")

    date_list = list(map(get_date, new_files))
    earliest_date = min(date_list)
    latest_date = max(date_list)

    # Check whether to merge
    # Check the number of files that are not merged
    if today.weekday() != backfill_merge_day:
        logger.info("No new files to merge; skipping merging")
        return
    elif (today - earliest_date).days <= check_nd:
        logger.info("Not enough days, skipping merging")
        return

    # Start to merge files
    logger.info("Merging files", start_date=earliest_date, end_date=latest_date)
    pdList = []
    for fn in new_files:
        df = pd.read_parquet(fn, engine='pyarrow')
        pdList.append(df)
    merged_file = pd.concat(pdList).sort_values(["time_value", "fips"])
    path = backfill_dir + "/claims_hosp_from_%s_to_%s.parquet"%(
        datetime.strftime(earliest_date, "%Y%m%d"),
        datetime.strftime(latest_date, "%Y%m%d"))
    merged_file.to_parquet(path, index=False)

    # Delete daily files once we have the merged one.
    if not test_mode:
        for fn in new_files:
            os.remove(fn)
    return


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
    new_files = sorted(Path(backfill_dir).glob("claims_hosp_*"))
    new_files.remove(Path(backfill_file))

    def get_file_with_date(files, issue_date) -> Union[Tuple[pathlib.Path, pathlib.Path], None]:
        """
        Give file with the missing date.

        Parameters
        ----------
        files: list of files in the backfill dir
        issue_date: the issue date of the file to be inserted into
        expand_flag: flag to indicate to check dates inclusive to from and to date in filenames

        Returns
        -------
        Tuple[pathlib.Path, pathlib.Path] if file is found, along with new filename
        after the insertion of the missing file

        None if no file is found
        """
        for filepath in files:
            pattern = re.findall(r"_(\d{8})", filepath.name)

            if len(pattern) == 2:
                start_date = datetime.strptime(pattern[0], "%Y%m%d")
                end_date = datetime.strptime(pattern[1], "%Y%m%d")
                # if date is in between from and to
                if start_date <= issue_date and end_date >= issue_date:
                    return filepath, filepath

            elif len(pattern) == 1:
                start_date = datetime.strptime(pattern[0], "%Y%m%d")
                if issue_date > start_date:
                    new_filename = filepath.name.replace(pattern[0], issue_date.strftime("%Y%m%d"))
                    new_filepath = Path(f"{filepath.parent}/{new_filename}")
                    return filepath, new_filepath

        for filepath in files:
            if len(pattern) == 2:
                start_date = datetime.strptime(pattern[0], "%Y%m%d")
                end_date = datetime.strptime(pattern[1], "%Y%m%d")

                # if date is either replacing a from date or a to date
                if issue_date == end_date + timedelta(days=1):
                    new_filename = filepath.name.replace(pattern[1], issue_date.strftime("%Y%m%d"))
                    new_filepath = Path(f"{filepath.parent}/{new_filename}")
                    return filepath, new_filepath

                elif issue_date == start_date - timedelta(days=1):
                    new_filename = filepath.name.replace(pattern[0], issue_date.strftime("%Y%m%d"))
                    new_filepath = Path(f"{filepath.parent}/{new_filename}")
                    return filepath, new_filepath

        return None, None

    file_path, new_file_path = get_file_with_date(new_files, issue_date)

    if file_path is None:
        logger.info("Issue date has no matching merged files", issue_date=issue_date.strftime("%Y-%m-%d"))
        return

    logger.info(
        "Adding missing date to merged file", issue_date=issue_date, filename=backfill_file, merged_filename=file_path
    )
    # Start to merge files
    file_name = file_path.stem
    merge_file = f"{file_path.parent}/{file_name}_after_merge.parquet"

    try:
        shutil.copyfile(file_path, merge_file)
        existing_df = pd.read_parquet(merge_file, engine="pyarrow")
        df = pd.read_parquet(backfill_file, engine="pyarrow")
        merged_df = pd.concat([existing_df, df]).sort_values(["time_value", "fips"])
        merged_df.to_parquet(merge_file, index=False)

    # pylint: disable=W0703
    except Exception as e:
        logger.info("Failed to merge existing backfill files", issue_date=issue_date.strftime("%Y-%m-%d"), msg=e)
        os.remove(merge_file)
        os.remove(backfill_file)
        return

    os.remove(file_path)
    os.rename(merge_file, new_file_path)
    return
