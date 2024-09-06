"""
Store backfill data.

Author: Jingjing Tang
Created: 2022-08-03

"""

import glob
import os
import re
import shutil
from datetime import datetime
from typing import Union

# third party
import pandas as pd
from delphi_utils import GeoMapper

from .config import Config

gmpr = GeoMapper()

def store_backfill_file(claims_filepath, _end_date, backfill_dir):
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

    path = backfill_dir + \
        "/claims_hosp_as_of_%s.parquet"%datetime.strftime(_end_date, "%Y%m%d")
    # Store intermediate file into the backfill folder
    backfilldata.to_parquet(path, index=False)
    return path


def merge_existing_backfill_files(backfill_dir, backfill_file, issue_date, logger):
    """
    Merge existing backfill with the patch data included.

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
    """
    new_files = glob.glob(backfill_dir + "/claims_hosp_*")

    def get_file_with_date(files) -> Union[str, None]:
        for filename in files:
            pattern = re.findall(r"\d{8}", filename)
            if len(pattern) == 2:
                start_date = datetime.strptime(pattern[0], "%Y%m%d")
                end_date = datetime.strptime(pattern[1], "%Y%m%d")
                if start_date <= issue_date or end_date <= issue_date:
                    return filename
        return ""

    file_name = get_file_with_date(new_files)

    if len(file_name) == 0:
        logger.info("patch file is too recent to merge", issue_date=issue_date.strftime("%Y-%m-%d"))
        return

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
    # pylint: disable=W0703:
    except Exception as e:
        os.remove(merge_file)
        logger.error(e)
    return


def merge_backfill_file(backfill_dir, backfill_merge_day, most_recent, test_mode=False, check_nd=25):
    """
    Merge ~4 weeks' backfill data into one file.

    Usually this function should merge 28 days' data into a new file so as to
    save the reading time when running the backfill pipelines. We set a softer
    threshold to allow flexibility in data delivery.
    Parameters
    ----------
    most_recent : datetime
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
    if most_recent.weekday() != backfill_merge_day or (most_recent - earliest_date).days <= check_nd:
        return

    # Start to merge files
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
