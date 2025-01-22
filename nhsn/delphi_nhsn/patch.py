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
    "patch_dir": "/Users/minhkhuele/Desktop/delphi/covidcast-indicators/nhsn/patch"
  }
}

It will generate data for the range of issue dates corresponding to source data files available in "backup_dir"
specified under "common", and store them in batch issue format under "patch_dir":
[name-of-patch]/issue_[issue-date]/nhsn/actual_data_file.csv
"""

from datetime import datetime
from os import makedirs
from pathlib import Path
from typing import List

from delphi_utils import get_structured_logger, read_params
from epiweeks import Week

from .run import run_module


def filter_source_files(source_files: List[Path]):
    """
    Filter patch files such that each element in the list is an unique epiweek with the latest issue date.

    Parameters
    ----------
    source_files

    Returns
    -------
    list of issue dates

    """
    epiweek_dict = dict()

    for file in source_files:
        if "prelim" not in file.stem:
            current_issue_date = datetime.strptime(file.name.split(".")[0], "%Y%m%d")
            epiweek = Week.fromdate(current_issue_date)
            epiweek_dict[epiweek] = file

    filtered_patch_list = list(epiweek_dict.values())
    return filtered_patch_list


def patch(params):
    """
    Run the doctor visits indicator for a range of issue dates.

    The range of issue dates is specified in params.json using the following keys:
    - "patch": Only used for patching data
        - "patch_dir": str, directory to write all issues output
    """
    logger = get_structured_logger("delphi_nhsn.patch", filename=params["common"]["log_filename"])

    source_files = sorted(Path(params["common"]["backup_dir"]).glob("*.csv.gz"))
    makedirs(params["patch"]["patch_dir"], exist_ok=True)

    logger.info(
        "Starting patching",
        patch_directory=params["patch"]["patch_dir"],
        start_issue=source_files[0].name.split(".")[0],
        end_issue=source_files[-1].name.split(".")[0],
    )

    patch_list = filter_source_files(source_files)
    for file in patch_list:
        issue_date_str = file.name.split(".")[0]
        logger.info("Running issue", issue_date=datetime.strptime(issue_date_str, "%Y%m%d").strftime("%Y-%m-%d"))
        params["patch"]["issue_date"] = issue_date_str
        # regardless of week date type or not the directory name must be issue_date_YYYYMMDD
        # conversion in done in acquisition
        current_issue_dir = f"{params['patch']['patch_dir']}/issue_{issue_date_str}/nhsn"
        makedirs(current_issue_dir, exist_ok=True)
        params["common"]["export_dir"] = current_issue_dir
        params["common"]["custom_run"] = True
        run_module(params, logger)


if __name__ == "__main__":
    patch(read_params())
