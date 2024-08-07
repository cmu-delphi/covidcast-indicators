"""
This module is used for patching data in the delphi_nssp package.

To use this module, you need to turn on the custom_run flag
and specify the range of issue dates in params.json, like so:

{
  "common": {
    "custom_run": true,
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

import sys
from datetime import datetime, timedelta
from os import makedirs, path

from delphi_utils import get_structured_logger, read_params
from epiweeks import Week

from .run import run_module


def good_patch_config(params, logger):
    """
    Check if the params.json file is correctly configured for patching.

    params: Dict[str, Any]
        Nested dictionary of parameters, typically loaded from params.json file.
    logger: Logger object
        Logger object to log messages.
    """
    good_patch_config = True
    custom_run = params["common"].get("custom_run", False)
    if not custom_run:
        logger.error("Calling patch.py without custom_run flag set true.")
        good_patch_config = False

    patch_config = params.get("patch", {})
    if patch_config == {}:
        logger.error("Custom flag is on, but patch section is missing.")
        good_patch_config = False
    else:
        required_patch_keys = ["start_issue", "end_issue", "patch_dir", "source_dir"]
        missing_keys = [key for key in required_patch_keys if key not in patch_config]
        if missing_keys:
            logger.error(f"Patch section is missing required key(s): {', '.join(missing_keys)}")
            good_patch_config = False
        else:
            try: #issue dates validity check
                start_issue = datetime.strptime(patch_config["start_issue"], "%Y-%m-%d")
                end_issue = datetime.strptime(patch_config["end_issue"], "%Y-%m-%d")
                if start_issue > end_issue:
                    logger.error("Start issue date is after end issue date.")
                    good_patch_config = False
            except ValueError:
                logger.error("Issue dates must be in YYYY-MM-DD format.")
                good_patch_config = False

            if not path.isdir(patch_config["source_dir"]):
                logger.error(f"Source directory {patch_config['source_dir']} does not exist.")
                good_patch_config = False

    if good_patch_config:
        logger.info("Good patch configuration.")
        return True
    else:
        logger.info("Bad patch configuration.")
        return False


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
    if not good_patch_config(params, logger):
        sys.exit(1)

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


if __name__ == "__main__":
    patch()
