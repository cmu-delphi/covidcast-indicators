"""
This module is used for patching data in the delphi_nhsn package.

To use this module, you need to specify the range of issue dates in params.json, like so:

{
  "common": {
    ...
  },
  "validation": {
    ...
  },
  "patch": {
    "patch_dir": "/Users/minhkhuele/Desktop/delphi/covidcast-indicators/nhsn/AprilPatch",
    "start_issue": "2024-04-20",
    "end_issue": "2024-04-21"
  }
}

It will generate data for that range of issue dates, and store them in batch issue format:
[name-of-patch]/issue_[issue-date]/doctor-visits/actual_data_file.csv
"""

from datetime import datetime
from os import makedirs
from pathlib import Path

from delphi_utils import get_structured_logger, read_params
from epiweeks import Week

from .run import run_module

def group_source_files(source_files):
    '''
    Group patch files such that each lists contains unique epiweek issue date.

    This allows for acquisitions break down patches files per unique epiweek
    NHSN has not been updating their data in a consistent fashion
    and in order to properly capture all the changes that happened, the patch files needs


    Parameters
    ----------
    source_files

    Returns
    -------

    '''
    days_in_week = 7
    patch_list = [[] for _ in range(days_in_week)]

    for file in source_files:
        if "prelim" not in file.stem:
            current_issue_date = datetime.strptime(file.name.split(".")[0], "%Y%m%d")
            weekday = current_issue_date.weekday()
            patch_list[weekday].append(current_issue_date)

    filtered_patch_list = [lst for lst in patch_list if lst]
    return filtered_patch_list


def patch():
    """
    Run the doctor visits indicator for a range of issue dates.

    The range of issue dates is specified in params.json using the following keys:
    - "patch": Only used for patching data
        - "start_date": str, YYYY-MM-DD format, first issue date
        - "end_date": str, YYYY-MM-DD format, last issue date
        - "patch_dir": str, directory to write all issues output
    """
    params = read_params()
    logger = get_structured_logger("delphi_nhsn.patch", filename=params["common"]["log_filename"])

    source_files = sorted(Path(params["common"]["backup_dir"]).glob("*.csv.gz"))

    patch_directory_prefix = params["patch"]["patch_dir"]
    patch_list = group_source_files(source_files)
    for idx, patch in enumerate(patch_list):
        start_issue = patch[0]
        end_issue = patch[-1]

        patch_directory = f"{patch_directory_prefix}_{idx}"
        params['patch']['patch_dir'] = patch_directory

        logger.info(
            "Starting patching",
            patch_directory=patch_directory,
            start_issue=start_issue.strftime("%Y-%m-%d"),
            end_issue=end_issue.strftime("%Y-%m-%d"),
        )

        makedirs(patch_directory, exist_ok=True)

        for issue_date in patch:
            current_issue_ew = Week.fromdate(issue_date)
            logger.info("Running issue", issue_date=issue_date.strftime("%Y-%m-%d"))
            params["patch"]["issue_date"] = issue_date.strftime("%Y%m%d")
            current_issue_dir = f"{params['patch']['patch_dir']}/issue_{current_issue_ew}/nhsn"
            makedirs(current_issue_dir, exist_ok=True)
            params["common"]["export_dir"] = current_issue_dir
            params["common"]["custom_run"] = True
            run_module(params, logger)


if __name__ == "__main__":
    patch()
