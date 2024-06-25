"""
This module is used for patching data in the delphi_doctor_visits package.

To use this module, you need to specify the range of issue dates in params.json.

It will generate data for that range of issue dates, and store them in batch issue format:
[name-of-patch]/issue_[issue-date]/doctor-visits/actual_data_file.csv
"""

from datetime import datetime, timedelta
from os import makedirs

from delphi_utils import get_structured_logger, read_params

from .run import run_module

if __name__ == "__main__":
    # Run the doctor visits indicator for a range of issue dates, specified in params.json using following keys:
    # - "patch": Only used for patching data
    #     - "start_date": str, YYYY-MM-DD format, first issue date
    #     - "end_date": str, YYYY-MM-DD format, last issue date
    #     - "patch_dir": str, directory to write all issues output
    params = read_params()
    logger = get_structured_logger("delphi_doctor_visits.patch", filename=params["common"]["log_filename"])

    start_issue = datetime.strptime(params["patch"]["start_issue"], "%Y-%m-%d")
    end_issue = datetime.strptime(params["patch"]["end_issue"], "%Y-%m-%d")

    logger.info(f"""Start patching {params["patch"]["patch_dir"]}""")
    logger.info(f"""Start issue: {start_issue.strftime("%Y-%m-%d")}""")
    logger.info(f"""End issue: {end_issue.strftime("%Y-%m-%d")}""")

    makedirs(params["patch"]["patch_dir"], exist_ok=True)

    current_issue = start_issue

    while current_issue <= end_issue:
        logger.info(f"""Running issue {current_issue.strftime("%Y-%m-%d")}""")

        params["patch"]["current_issue"] = current_issue.strftime("%Y-%m-%d")

        current_issue_yyyymmdd = current_issue.strftime("%Y%m%d")
        current_issue_dir = f"""{params["patch"]["patch_dir"]}/issue_{current_issue_yyyymmdd}/doctor-visits"""
        makedirs(f"{current_issue_dir}", exist_ok=True)
        params["common"]["export_dir"] = f"""{current_issue_dir}"""

        run_module(params, logger)
        current_issue += timedelta(days=1)
