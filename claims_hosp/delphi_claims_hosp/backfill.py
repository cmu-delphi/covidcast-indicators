"""
Store backfill data.

Author: Jingjing Tang
Created: 2022-08-03

"""
import glob
import os
import re
from datetime import datetime, timedelta
from pathlib import Path

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
        logger: structlog.BoundLogger

    Returns:
        str, path of the file written.
    """
    backfilldata = pd.read_csv(
        claims_filepath,
        usecols=Config.CLAIMS_DTYPES.keys(),
        dtype=Config.CLAIMS_DTYPES,
        parse_dates=[Config.CLAIMS_DATE_COL],
    )
    backfilldata.rename(
        {
            "ServiceDate": "time_value",
            "PatCountyFIPS": "fips",
            "Denominator": "den",
            "Covid_like": "num",
            "Flu1": "num_flu",
        },
        axis=1,
        inplace=True,
    )
    backfilldata = gmpr.add_geocode(
        backfilldata, from_code="fips", new_code="state_id", from_col="fips", new_col="state_id"
    )
    #Store one year's backfill data
    if _end_date.day == 29 and _end_date.month == 2:
        _start_date = datetime(_end_date.year-1, 2, 28)
    else:
        _start_date = _end_date.replace(year=_end_date.year - 1)
    selected_columns = ["time_value", "fips", "state_id", "den", "num", "num_flu"]
    backfilldata = backfilldata.loc[
        (backfilldata["time_value"] >= _start_date) & (~backfilldata["fips"].isnull()), selected_columns
    ]

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
    logger.info("Stored source data in parquet", filename=path)
    return path

def merge_backfill_file(backfill_dir, backfill_merge_day, today, logger,
                        test_mode=False, check_nd=25):
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
    logger: structlog.BoundLogger
    test_mode: bool
    check_nd: int
        The criteria of the number of unmerged files. Ideally, we want the
        number to be 28, but we use a looser criteria from practical
        considerations
    """
    new_files = glob.glob(backfill_dir + "/claims_hosp_as_of_*")
    if len(new_files) == 0: # if no any daily file is stored
        logger.info("No unmerged backfill files, skipping merge")
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
        logger.info("Not a merge day, skipping merge")
        return
    if (today - earliest_date).days <= check_nd:
        logger.info("Not enough unmerged days, skipping merge",
                    earliest_date=earliest_date.strftime("%Y-%m-%d"))
        return

    # Start to merge files
    logger.info("Merging backfill files",
                start_date=earliest_date.strftime("%Y-%m-%d"),
                end_date=latest_date.strftime("%Y-%m-%d"))
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


MERGED_FILENAME = re.compile(r"^claims_hosp_from_(\d{8})_to_(\d{8})\.parquet$")


def merged_filename(start_date, end_date):
    """Build the name of the merged backfill file spanning the given dates."""
    return "claims_hosp_from_%s_to_%s.parquet" % (
        datetime.strftime(start_date, "%Y%m%d"),
        datetime.strftime(end_date, "%Y%m%d"))


def get_merged_file_for_date(backfill_dir, issue_date):
    """
    Find the merged backfill file that a patched issue date belongs in.

    An issue inside an existing [from, to] span goes back into that file under
    its current name. An issue immediately before or after a span extends it,
    so the file has to be renamed. An issue that touches no span at all is left
    to the regular weekly merge.

    Parameters
    ----------
    backfill_dir : str
        specified path to store backfill files.
    issue_date : datetime
        The issue date being patched.

    Returns
    -------
    (Path, Path) of the merged file to update and the name it should carry
    afterwards, or (None, None) if no merged file covers or abuts the date.
    """
    spans = []
    for filepath in sorted(Path(backfill_dir).glob("claims_hosp_from_*_to_*.parquet")):
        match = MERGED_FILENAME.match(filepath.name)
        if not match:
            continue
        spans.append((filepath,
                      datetime.strptime(match.group(1), "%Y%m%d"),
                      datetime.strptime(match.group(2), "%Y%m%d")))

    for filepath, start_date, end_date in spans:
        if start_date <= issue_date <= end_date:
            return filepath, filepath

    # the issue date fell on the edge of an outage, just outside a merged span
    for filepath, start_date, end_date in spans:
        if issue_date == end_date + timedelta(days=1):
            return filepath, filepath.parent / merged_filename(start_date, issue_date)
        if issue_date == start_date - timedelta(days=1):
            return filepath, filepath.parent / merged_filename(issue_date, end_date)

    return None, None


def merge_existing_backfill_files(backfill_dir, backfill_file, issue_date, logger):
    """
    Add a patched issue to the merged backfill file that already covers its date.

    When the indicator fails for a day, that day is missing from the backfill
    files. By the time we patch it the surrounding days have usually been merged
    already, so the newly stored daily file has to be folded into the merged
    file rather than left for the weekly merge.

    Rows already carrying this issue date are dropped before the merge, so
    re-running a patch over the same date does not double up the data.

    Parameters
    ----------
    backfill_dir : str
        specified path to store backfill files.
    backfill_file : str
        daily backfill file just written for the patched issue.
    issue_date : datetime
        The issue date being patched.
    logger: structlog.BoundLogger
    """
    file_path, new_file_path = get_merged_file_for_date(backfill_dir, issue_date)

    if file_path is None:
        logger.info("No merged backfill file covers this issue date; "
                    "leaving the daily file for the next scheduled merge",
                    issue_date=issue_date.strftime("%Y-%m-%d"))
        return

    logger.info("Adding patched issue to merged backfill file",
                issue_date=issue_date.strftime("%Y-%m-%d"),
                filename=str(backfill_file),
                merged_filename=str(file_path))

    merge_file = file_path.parent / f"{file_path.stem}_after_merge.parquet"
    try:
        existing_df = pd.read_parquet(file_path, engine="pyarrow")
        df = pd.read_parquet(backfill_file, engine="pyarrow")
        issue_str = datetime.strftime(issue_date, "%Y-%m-%d")
        already_present = existing_df["issue_date"] == issue_str
        if already_present.any():
            logger.info("Replacing rows already present for this issue date",
                        issue_date=issue_str, row_count=int(already_present.sum()))
            existing_df = existing_df[~already_present]
        merged_df = pd.concat([existing_df, df]).sort_values(["time_value", "fips"])
        merged_df.to_parquet(merge_file, index=False)
    except Exception as e:  # pylint: disable=broad-except
        # leave the daily file in place; the patched data is still recoverable
        logger.error("Failed to merge patched issue into existing backfill file",
                     issue_date=issue_date.strftime("%Y-%m-%d"), msg=str(e))
        if os.path.exists(merge_file):
            os.remove(merge_file)
        return

    os.remove(file_path)
    os.replace(merge_file, new_file_path)
    os.remove(backfill_file)
    logger.info("Merged patched issue into backfill file", merged_filename=str(new_file_path))
    return
