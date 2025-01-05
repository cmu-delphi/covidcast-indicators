"""
This module is used for patching data in the delphi_nssp package.

The code assume user can use key-based auth to access prod server
where historical source data is stored.

To use this module, configure params.json like so:

{
  "common": {
    "custom_run": true,
    ...
  },
  "validation": {
    ...
  },
  "patch": {
    "source_dir": "delphi/covidcast-indicators/nssp/source_data",
    "user": "username",
    "patch_dir": "delphi/covidcast-indicators/nssp/AprilPatch",
    "start_issue": "2024-04-20",
    "end_issue": "2024-04-21",
  }
}

In this params.json, we
- Turn on the "custom_run" flag under "common"
- Add "patch" section, which contains:
    + "source_dir": the local directory where source data is downloaded to
    + "user": the username to log in to the remote server where source data is backed up
    + "patch_dir": the local directory where to write all patch issues output
    + "start_date": str, YYYY-MM-DD format, first issue date
    + "end_date": str, YYYY-MM-DD format, last issue date

if "source_dir" doesn't exist locally or has no files in it, we download source data to source_dir
else, we assume all needed source files are already in source_dir.

This module will generate data for that range of issue dates, and store them in batch issue format in the patch_dir:
[patch_dir]/issue_[issue-date]/nssp/actual_data_file.csv
"""

import sys
from datetime import datetime, timedelta
from os import listdir, makedirs, path, getcwd
from shutil import rmtree

from delphi_utils import get_structured_logger, read_params
from epiweeks import Week

from .pull import get_source_data
from .run import run_module


def good_patch_config(params, logger):
    """
    Check if the params.json file is correctly configured for patching.

    params: Dict[str, Any]
        Nested dictionary of parameters, typically loaded from params.json file.
    logger: Logger object
        Logger object to log messages.
    """
    valid_config = True
    custom_run = params["common"].get("custom_run", False)
    if not custom_run:
        logger.error("Calling patch.py without custom_run flag set true.")
        valid_config = False

    patch_config = params.get("patch", {})
    if patch_config == {}:
        logger.error("Custom flag is on, but patch section is missing.")
        valid_config = False
    else:
        required_patch_keys = ["start_issue", "end_issue", "patch_dir", "source_dir", "user"]
        missing_keys = [key for key in required_patch_keys if key not in patch_config]
        if missing_keys:
            logger.error("Patch section is missing required key(s)", missing_keys=missing_keys)
            valid_config = False
        else:
            try:  # issue dates validity check
                start_issue = datetime.strptime(patch_config["start_issue"], "%Y-%m-%d")
                end_issue = datetime.strptime(patch_config["end_issue"], "%Y-%m-%d")
                if start_issue > end_issue:
                    logger.error("Start issue date is after end issue date.")
                    valid_config = False
            except ValueError:
                logger.error("Issue dates must be in YYYY-MM-DD format.")
                valid_config = False

    if valid_config:
        logger.info("Good patch configuration.")
        return True
    logger.info("Bad patch configuration.")
    return False


def patch():
    """
    Run the nssp indicator for a range of issue dates.
    """
    params = read_params()
    logger = get_structured_logger("delphi_nssp.patch", filename=params["common"]["log_filename"])
    if not good_patch_config(params, logger):
        sys.exit(1)

    source_dir = params["patch"]["source_dir"]
    download_source = False
    if not path.isdir(source_dir) or not listdir(source_dir): #no source dir or empty source dir
        download_source = True
        get_source_data(params, logger)
    else:
        logger.info("Source data already exists locally.")

    start_issue = datetime.strptime(params["patch"]["start_issue"], "%Y-%m-%d")
    end_issue = datetime.strptime(params["patch"]["end_issue"], "%Y-%m-%d")

    logger.info(start_issue=start_issue.strftime("%Y-%m-%d"))
    logger.info(end_issue=end_issue.strftime("%Y-%m-%d"))
    logger.info(source_dir=source_dir)
    logger.info(patch_dir=params["patch"]["patch_dir"])

    makedirs(params["patch"]["patch_dir"], exist_ok=True)

    current_issue = start_issue
    while current_issue <= end_issue:
        logger.info("patching issue", issue_date=current_issue.strftime("%Y%m%d"))

        current_issue_source_csv = f"""{source_dir}/{current_issue.strftime("%Y%m%d")}.csv.gz"""
        if not path.isfile(current_issue_source_csv):
            logger.info("No source data at this path", current_issue_source_csv=current_issue_source_csv)
            current_issue += timedelta(days=1)
            continue

        params["patch"]["current_issue"] = current_issue.strftime("%Y%m%d")

        # current_issue_date can be different from params["patch"]["current_issue"]
        # due to weekly cadence of nssp data. For weekly sources, issue dates in our
        # db matches with first date of epiweek that the reporting date falls in,
        # rather than reporting date itself.
        current_issue_date = Week.fromdate(current_issue).startdate()
        current_issue_dir = f"""{params["patch"]["patch_dir"]}/issue_{current_issue_date.strftime("%Y%m%d")}/nssp"""
        makedirs(f"{current_issue_dir}", exist_ok=True)
        params["common"]["export_dir"] = f"""{current_issue_dir}"""

        run_module(params, logger)
        current_issue += timedelta(days=1)

    # if download_source:
    #     rmtree(source_dir)


if __name__ == "__main__":
    patch()
