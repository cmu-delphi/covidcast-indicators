"""
This module is used for patching data in the delphi_google_symptom package.

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
[name-of-patch]/issue_[issue-date]/doctor-visits/actual_data_file.csv
"""

from datetime import datetime, timedelta
from os import makedirs

from delphi_utils import get_structured_logger, read_params

from .date_utils import generate_patch_dates
from .run import run_module


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
    logger = get_structured_logger("delphi_google_symptom.patch", filename=params["common"]["log_filename"])

    start_issue = datetime.strptime(params["patch"]["start_issue"], "%Y-%m-%d")
    end_issue = datetime.strptime(params["patch"]["end_issue"], "%Y-%m-%d")

    logger.info(f"""Start patching {params["patch"]["patch_dir"]}""")
    logger.info(f"""Start issue: {start_issue.strftime("%Y-%m-%d")}""")
    logger.info(f"""End issue: {end_issue.strftime("%Y-%m-%d")}""")

    makedirs(params["patch"]["patch_dir"], exist_ok=True)

    current_issue = start_issue

    patch_dates = generate_patch_dates(params)

    for issue_date_info in patch_dates:
        issue_date, daterange = list(*issue_date_info.items())
        logger.info(f"""Running issue {issue_date.strftime("%Y-%m-%d")}""")
        current_issue_yyyymmdd = issue_date.strftime("%Y%m%d")
        current_issue_dir = f"""{params["patch"]["patch_dir"]}/issue_{current_issue_yyyymmdd}/google-symptom"""
        makedirs(f"{current_issue_dir}", exist_ok=True)
        params["common"]["export_dir"] = f"""{current_issue_dir}"""

        params["indicator"]["export_start_date"] = daterange[0].strftime("%Y-%m-%d")
        params["indicator"]["export_end_date"] = daterange[1].strftime("%Y-%m-%d")
        run_module(params, logger)
        current_issue += timedelta(days=1)


if __name__ == "__main__":
    patch()
