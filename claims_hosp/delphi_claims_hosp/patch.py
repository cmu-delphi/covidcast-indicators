"""
This module is used for patching data in the delphi_claims_hosp package.

To use this module, you need to specify the range of issue dates in params.json, like so:

{
  "common": {
    "custom_flag" : true,
    ...
  },
  "validation": {
    ...
  },
  "patch": {
    "patch_dir": "/covidcast-indicators/hopspital-admissions/patch",
    "start_issue": "2024-04-20",
    "end_issue": "2024-04-21"
  }
}

It will generate data for that range of issue dates, and store them in batch issue format:
[name-of-patch]/issue_[issue-date]/doctor-visits/actual_data_file.csv
"""

from datetime import datetime, timedelta
from os import makedirs

from delphi_utils import get_structured_logger, read_params

from .run import run_module


def patch():
    """
    Run the hospital-admissions indicator for a range of issue dates.

    The range of issue dates is specified in params.json using the following keys:
    - "patch": Only used for patching data
        - "start_date": str, YYYY-MM-DD format, first issue date
        - "end_date": str, YYYY-MM-DD format, last issue date
        - "patch_dir": str, directory to write all issues output
    """
    params = read_params()
    logger = get_structured_logger("delphi_claims_hosp.patch", filename=params["common"]["log_filename"])

    start_issue = datetime.strptime(params["patch"]["start_issue"], "%Y-%m-%d")
    end_issue = datetime.strptime(params["patch"]["end_issue"], "%Y-%m-%d")

    logger.info(
        "Starting patching",
        patch_directory=params["patch"]["patch_dir"],
        start_issue=start_issue.strftime("%Y-%m-%d"),
        end_issue=end_issue.strftime("%Y-%m-%d"),
    )

    makedirs(params["patch"]["patch_dir"], exist_ok=True)

    current_issue = start_issue

    while current_issue <= end_issue:
        logger.info("Running issue", issue_date=current_issue.strftime("%Y-%m-%d"))

        params["patch"]["current_issue"] = current_issue.strftime("%Y-%m-%d")

        current_issue_yyyymmdd = current_issue.strftime("%Y%m%d")
        current_issue_dir = f"""{params["patch"]["patch_dir"]}/issue_{current_issue_yyyymmdd}/hospital-admissions"""
        makedirs(f"{current_issue_dir}", exist_ok=True)
        params["common"]["export_dir"] = f"""{current_issue_dir}"""

        run_module(params, logger)
        current_issue += timedelta(days=1)


if __name__ == "__main__":
    patch()
