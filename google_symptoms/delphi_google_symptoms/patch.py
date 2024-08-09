"""
This module is used for patching data in the delphi_google_symptom package.

To use this module, you need to specify the range of issue dates in params.json, like so:

{
  "common": {
    ...
    "custom_run": true
  },
  "validation": {
    ...
  },
  "patch": {
    "patch_dir": ".../covidcast-indicators/google-symptoms/AprilPatch",
    "start_issue": "2024-04-20",
    "end_issue": "2024-04-21"
  }
}

It will generate data for that range of issue dates, and store them in batch issue format:
[params patch_dir]/issue_[issue-date]/google-symptoms/xxx.csv
"""

from datetime import datetime, timedelta
from os import makedirs

from delphi_utils import get_structured_logger, read_params

from .date_utils import generate_patch_dates
from .run import run_module


def patch(params):
    """
    Run the google symptoms indicator for a range of issue dates.

    Parameters
    ----------
    params
        Dictionary containing indicator configuration. Expected to have the following structure:
    - "common":
        - "export_dir": str, directory to write output
        - "log_exceptions" (optional): bool, whether to log exceptions to file
        - "log_filename" (optional): str, name of file to write logs
    - "indicator":
        - "export_start_date": str, YYYY-MM-DD format, date from which to export data
        - "num_export_days": int, number of days before end date (today) to export
        - "path_to_bigquery_credentials": str, path to BigQuery API key and service account
            JSON file
    - "patch": Only used for patching data
        - "start_date": str, YYYY-MM-DD format, first issue date
        - "end_date": str, YYYY-MM-DD format, last issue date
        - "patch_dir": str, directory to write all issues output
    """
    logger = get_structured_logger("delphi_google_symptom.patch", filename=params["common"]["log_filename"])

    issue_date = datetime.strptime(params["patch"]["start_issue"], "%Y-%m-%d")
    end_issue = datetime.strptime(params["patch"]["end_issue"], "%Y-%m-%d")

    logger.info(f"""Start patching {params["patch"]["patch_dir"]}""")
    logger.info(f"""Start issue: {issue_date.strftime("%Y-%m-%d")}""")
    logger.info(f"""End issue: {end_issue.strftime("%Y-%m-%d")}""")

    makedirs(params["patch"]["patch_dir"], exist_ok=True)

    patch_dates = generate_patch_dates(params)

    while issue_date <= end_issue:
        logger.info(f"""Running issue {issue_date.strftime("%Y-%m-%d")}""")

        # Output dir setup
        current_issue_yyyymmdd = issue_date.strftime("%Y%m%d")
        current_issue_dir = f"""{params["patch"]["patch_dir"]}/issue_{current_issue_yyyymmdd}/google-symptoms"""
        makedirs(f"{current_issue_dir}", exist_ok=True)

        params["common"]["export_dir"] = f"""{current_issue_dir}"""
        params["indicator"]["custom_run"] = True

        date_settings = patch_dates[issue_date]

        params["indicator"]["export_start_date"] = date_settings["export_start_date"].strftime("%Y-%m-%d")
        params["indicator"]["export_end_date"] = date_settings["export_end_date"].strftime("%Y-%m-%d")
        params["indicator"]["num_export_days"] = date_settings["num_export_days"]

        run_module(params, logger)

        issue_date += timedelta(days=1)


if __name__ == "__main__":
    patch(read_params())
