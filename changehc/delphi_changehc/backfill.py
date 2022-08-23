"""
Store backfill data.

Author: Jingjing Tang
Created: 2022-08-03
"""
import glob
import os
from datetime import datetime
# third party
import pandas as pd


def store_backfill_file(df, _end_date, backfill_dir, numtype, geo, weekday):
    """
    Store county level backfill data into backfill_dir.

    Parameter:
        df: pd.DataFrame
            Store the county level raw file for backfill
        _end_date: datetime
            The most recent date when the raw data is received
        backfill_dir: str
            specified path to store backfill files.
        numtype: str
            indicate the type of the data
        geo: str
            geo level
        weekday: bool
    """
    # We only need to run it once for a numtype
    if geo != "county":
        return
    if weekday:
        return

    backfilldata = df.reset_index().copy()
    backfilldata.rename({"timestamp": "time_value"}, axis=1, inplace=True)
    #Store one year's backfill data
    _start_date = _end_date.replace(year=_end_date.year-1)
    selected_columns = ['time_value', 'fips',
                        'num', 'den']
    backfilldata = backfilldata.loc[backfilldata["time_value"] >= _start_date,
                                    selected_columns]
    path = backfill_dir + \
        "/changehc_%s_as_of_%s.parquet"%(numtype, datetime.strftime(_end_date, "%Y%m%d"))
    # Store intermediate file into the backfill folder
    backfilldata.to_parquet(path)

def merge_backfill_file(backfill_dir, numtype, geo, weekday, backfill_merge_day,
                        today, test_mode=False, check_nd=25):
    """
    Merge ~4 weeks' backfill data into one file.

    Usually this function should merge 28 days' data into a new file so as to
    save the reading time when running the backfill pipelines. We set a softer
    threshold to allow flexibility in data delivery.
    Parameters
    ----------
    today : datetime
        The current dropdate
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
    numtype: str
            indicate the type of the data
        geo: str
            geo level
        weekday: bool
    """
    # We only need to run it once for a numtype
    if geo != "county":
        return
    if weekday:
        return

    new_files = glob.glob(backfill_dir + "/changehc_%s_as_of_*"%numtype)

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
    if today.weekday() != backfill_merge_day or (today-earliest_date).days <= check_nd:
        return

    # Start to merge files
    pdList = []
    for fn in new_files:
        df = pd.read_parquet(fn, engine='pyarrow')
        issue_date = get_date(fn)
        df["issue_date"] = issue_date
        df["lag"] = [(issue_date - x).days for x in df["time_value"]]
        pdList.append(df)
    merged_file = pd.concat(pdList).sort_values(["time_value", "fips"])
    path = backfill_dir + "/changehc_%s_from_%s_to_%s.parquet"%(
        numtype,
        datetime.strftime(earliest_date, "%Y%m%d"),
        datetime.strftime(latest_date, "%Y%m%d"))
    merged_file.to_parquet(path)

    # Delete daily files once we have the merged one.
    if not test_mode:
        for fn in new_files:
            os.remove(fn)
    return
