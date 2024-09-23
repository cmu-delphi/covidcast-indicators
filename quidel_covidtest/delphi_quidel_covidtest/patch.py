"""
This module is used for patching data in the delphi_doctor_visits package.

To use this module, you need to specify the range of issue dates in params.json, like so:

{
  "common": {
    ...
  },
  "validation": {
    ...
  },
  "patch": {
    "patch_dir": "/Users/minhkhuele/Desktop/delphi/covidcast-indicators/doctor_visits/AprilPatch",
    "start_issue": "2024-04-20",
    "end_issue": "2024-04-21"
  }
}

It will generate data for that range of issue dates, and store them in batch issue format:
[name-of-patch]/issue_[issue-date]/quidel_covidtest/actual_data_file.csv
"""
import time
from datetime import datetime, timedelta
from os import makedirs
from pathlib import Path

from delphi_utils import get_structured_logger, read_params

from .pull import preprocess_new_data
from .run import run_module

def grab_source(params, logger):
    """
    Grab source data for patch range ahead of time.

    Parameters
    ----------
    params
    logger

    Returns
    -------

    """
    start_issue = params["patch"]["start_issue"]
    end_issue = params["patch"]["end_issue"]
    end_date = datetime.strptime(end_issue, "%Y-%m-%d")

    cache_dir = params["indicator"]['input_cache_dir']
    filename = f"{cache_dir}/pulled_until_{(end_date + timedelta(days=1)).strftime('%Y%m%d')}.csv"
    if Path(filename).is_file():
        return
    start_time = time.time()
    start_date = datetime.strptime(start_issue, "%Y-%m-%d")
    df, _ = preprocess_new_data(start_date, end_date, params["indicator"], params["indicator"]["test_mode"], logger)
    df.to_csv(filename, index=False)
    logger.info("Completed cache file update",
                start_issue=start_issue,
                end_issue = end_issue,
                elapsed_time_in_seconds = round(time.time() - start_time, 2))

def patch():
    """
    Run the quidel_covidtest indicator for a range of issue dates.

    The range of issue dates is specified in params.json using the following keys:
    - "patch": Only used for patching data
        - "start_date": str, YYYY-MM-DD format, first issue date
        - "end_date": str, YYYY-MM-DD format, last issue date
        - "patch_dir": str, directory to write all issues output
    """
    params = read_params()
    logger = get_structured_logger("delphi_quidel_covidtest.patch", filename=params["common"]["log_filename"])

    start_issue = datetime.strptime(params["patch"]["start_issue"], "%Y-%m-%d")
    end_issue = datetime.strptime(params["patch"]["end_issue"], "%Y-%m-%d")

    logger.info(
        "Starting patching",
        patch_directory=params["patch"]["patch_dir"],
        start_issue=start_issue.strftime("%Y-%m-%d"),
        end_issue=end_issue.strftime("%Y-%m-%d"),
    )
    params["common"]["custom_run"] = True
    makedirs(params["patch"]["patch_dir"], exist_ok=True)
    grab_source(params, logger)
    current_issue = start_issue

    while current_issue <= end_issue:
        logger.info("Running issue", issue_date=current_issue.strftime("%Y-%m-%d"))

        current_issue_yyyymmdd = current_issue.strftime("%Y%m%d")
        current_issue_dir = f"""{params["patch"]["patch_dir"]}/issue_{current_issue_yyyymmdd}/quidel_covidtest"""
        makedirs(f"{current_issue_dir}", exist_ok=True)
        params["common"]["export_dir"] = f"""{current_issue_dir}"""
        params["indicator"]["pull_start_date"] = current_issue.strftime("%Y-%m-%d")

        run_module(params, logger)
        current_issue += timedelta(days=1)


if __name__ == "__main__":
    patch()
