"""
This module is used for creating data patches for a range of issues

To use this module, you need to specify the desired range of issue dates in
params.json, like so:

{
  "common": {
    ...
  },
  "validation": {
    ...
  },
  "patch": {
    "patch_dir": "/local/path/to/dir/patch_dir",
    "start_issue": "2024-04-20",
    "end_issue": "2024-04-21"
  }
}

It will generate data for that range of issue dates, and store them in batch
issue format (patch_dir/issue_[issue-date]/[data-source-name]/actual_data_file.csv).
For example, for doctor-visits:

AprilPatch/
    issue_20240420/
        doctor-visits/
            20240408_state_smoothed_adj_cli.csv
            20240408_state_smoothed_cli.csv
            ...
    issue_20240421/
        doctor-visits/
            20240409_state_smoothed_adj_cli.csv
            20240408_state_smoothed_cli.csv
            ...
"""

from datetime import datetime, timedelta
from os import makedirs

from delphi_utils import get_structured_logger, read_params

from .run import run_module
from .constants import DATA_SOURCE_NAME


def patch():
    """
    Run the indicator for a range of issue dates.

    The range of issue dates is specified in params.json using the following keys:
    - "patch": Only used for patching data
        - "start_date": str, YYYY-MM-DD format, first issue date
        - "end_date": str, YYYY-MM-DD format, last issue date
        - "patch_dir": str, directory to write all issues output to
    """
    params = read_params()
    logger = get_structured_logger(f"delphi_{DATA_SOURCE_NAME}.patch", filename=params["common"]["log_filename"])

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
        current_issue_dir = f"""{params["patch"]["patch_dir"]}/issue_{current_issue_yyyymmdd}/{DATA_SOURCE_NAME}"""
        makedirs(f"{current_issue_dir}", exist_ok=True)
        params["common"]["export_dir"] = f"""{current_issue_dir}"""

        run_module(params, logger)
        current_issue += timedelta(days=1)


if __name__ == "__main__":
    patch()
