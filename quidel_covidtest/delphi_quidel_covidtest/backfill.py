# -*- coding: utf-8 -*-
"""Store backfill data."""
import os
import glob
from datetime import datetime

import pandas as pd

from delphi_utils import GeoMapper


gmpr = GeoMapper()

def store_backfill_file(df, _end_date, backfill_dir):
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
    _start_date = _end_date.replace(year=_end_date.year-1)
    selected_columns = ['time_value', 'fips',
                        'den_total', 'num_total',
                        'num_age_0_4', 'den_age_0_4',
                        'num_age_5_17', 'den_age_5_17',
                        'num_age_18_49', 'den_age_18_49',
                        'num_age_50_64', 'den_age_50_64',
                        'num_age_65plus', 'den_age_65plus',
                        'num_age_0_17', 'den_age_0_17']
    backfilldata = backfilldata.loc[backfilldata["time_value"] >= _start_date,
                                    selected_columns]
    path = backfill_dir + \
        "/quidel_covidtest_as_of_%s.parquet"%datetime.strftime(_end_date, "%Y%m%d")
    # Store intermediate file into the backfill folder
    backfilldata.to_parquet(path)

def merge_backfill_file(backfill_dir, backfill_merge_day, today,
                        test_mode=False, check_nd=25):
    """
    Merge ~4 weeks' backfill data into one file.

    Usually this function should merge 28 days' data into a new file so as to
    save the reading time when running the backfill pipelines. We

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
    new_files = glob.glob(backfill_dir + "/quidel_covidtest_as_of_*")

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
    path = backfill_dir + "/quidel_covidtest_from_%s_to_%s.parquet"%(
        datetime.strftime(earliest_date, "%Y%m%d"),
        datetime.strftime(latest_date, "%Y%m%d"))
    merged_file.to_parquet(path)

    # Delete daily files once we have the merged one.
    if not test_mode:
        for fn in new_files:
            os.remove(fn)
    return
