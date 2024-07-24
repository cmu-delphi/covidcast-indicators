"""
This module is used for patching data in the delphi_nssp package.

To use this module, you need to specify the range of issue dates in params.json, like so:

{
  "common": {
    ...
  },
  "validation": {
    ...
  },
  "patch": {
    "patch_dir": "/Users/minhkhuele/Desktop/delphi/covidcast-indicators/nssp/AprilPatch",
    "start_issue": "2024-04-20",
    "end_issue": "2024-04-21",
    "source_dir": "/Users/minhkhuele/Desktop/delphi/covidcast-indicators/nssp/source_data"
  }
}

It will generate data for that range of issue dates, and store them in batch issue format:
[name-of-patch]/issue_[issue-date]/nssp/actual_data_file.csv
"""

from datetime import datetime, timedelta
from os import makedirs, path

from delphi_utils import get_structured_logger, read_params
from epiweeks import Week

from .run import run_module


def patch():
    """
    Run the nssp indicator for a range of issue dates.

    The range of issue dates is specified in params.json using the following keys:
    - "patch": Only used for patching data
        - "start_date": str, YYYY-MM-DD format, first issue date
        - "end_date": str, YYYY-MM-DD format, last issue date
        - "patch_dir": str, directory to write all issues output
        - "source_dir": str, directory to read source data from.
    """
    params = read_params()
    logger = get_structured_logger("delphi_nssp.patch", filename=params["common"]["log_filename"])

    start_issue = datetime.strptime(params["patch"]["start_issue"], "%Y-%m-%d")
    end_issue = datetime.strptime(params["patch"]["end_issue"], "%Y-%m-%d")

    logger.info(f"""Start patching {params["patch"]["patch_dir"]}""")
    logger.info(f"""Start issue: {start_issue.strftime("%Y-%m-%d")}""")
    logger.info(f"""End issue: {end_issue.strftime("%Y-%m-%d")}""")
    logger.info(f"""Source from: {params["patch"]["source_dir"]}""")
    logger.info(f"""Output to: {params["patch"]["patch_dir"]}""")

    makedirs(params["patch"]["patch_dir"], exist_ok=True)

    current_issue = start_issue
    while current_issue <= end_issue:
        logger.info(f"""Running issue {current_issue.strftime("%Y-%m-%d")}""")

        current_issue_source_csv = (
            f"""{params.get("patch", {}).get("source_dir")}/{current_issue.strftime("%Y-%m-%d")}.csv"""
        )
        if not path.isfile(current_issue_source_csv):
            logger.info(f"No source data at {current_issue_source_csv}")
            current_issue += timedelta(days=1)
            continue

        params["patch"]["current_issue"] = current_issue.strftime("%Y-%m-%d")

        current_issue_week = Week.fromdate(current_issue)
        current_issue_dir = f"""{params["patch"]["patch_dir"]}/issue_{current_issue_week}/nssp"""
        makedirs(f"{current_issue_dir}", exist_ok=True)
        params["common"]["export_dir"] = f"""{current_issue_dir}"""

        run_module(params, logger)
        current_issue += timedelta(days=1)


if __name__ == "__main__":
    patch()
