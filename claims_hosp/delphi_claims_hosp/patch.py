"""
This module is used for patching data in the delphi_claims_hosp package.

An issue is a whole re-run of the indicator against the drop that arrived that
day, not one day of data: each issue re-emits the same window of time_values the
daily run would have. That window is n_backfill_days deep unless
indicator.start_date is set, which overrides it and can make it much deeper.

Each issue pulls the drop that arrived on its issue date from the ftp server, so
patching needs working ftp credentials. How far back the server keeps drops is a
property of the server; for older issues, stage the drops
(EDI_AGG_INPATIENT_DDMMYYYY_HHMM{timezone}.csv.gz) in "input_dir" yourself and
the downloader will skip over them. A patch leaves "input_dir" populated when it
finishes rather than clearing it the way a daily run does, so give patches their
own "input_dir" if you don't want the drops mixed in with the daily staging dir.

Issue dates with no drop available are logged and skipped.

To use this module, configure params.json like so:

{
  "common": {
    "custom_run": true,
    ...
  },
  "indicator": {
    "input_dir": "/common/covidcast/archive/hospital-admissions",
    ...
  },
  "patch": {
    "patch_dir": "/covidcast-indicators/claims_hosp/AprilPatch",
    "start_issue": "2024-04-20",
    "end_issue": "2024-04-21"
  }
}

In this params.json, we
- Turn on the "custom_run" flag under "common"
- Add a "patch" section, which contains:
    + "patch_dir": the local directory to write all patch issues output
    + "start_issue": str, YYYY-MM-DD format, first issue date
    + "end_issue": str, YYYY-MM-DD format, last issue date

It will generate data for that range of issue dates, and store them in batch
issue format:
[patch_dir]/issue_[issue-date]/hospital-admissions/actual_data_file.csv

If "generate_backfill_files" is on, each patched issue is also stored as a
daily backfill file and folded back into the merged backfill file that covers
its date. See merge_existing_backfill_files in backfill.py.
"""

import sys
from datetime import datetime, timedelta
from os import makedirs, path, rmdir
from shutil import rmtree

from delphi_utils import get_structured_logger, read_params

from .get_latest_claims_name import NoDropError
from .run import run_module


def good_patch_config(params, logger):
    """
    Check if the params.json file is correctly configured for patching.

    params: Dict[str, Any]
        Nested dictionary of parameters, typically loaded from params.json file.
    logger: structlog.BoundLogger
    """
    valid_config = True
    if not params["common"].get("custom_run", False):
        logger.error("Calling patch.py without custom_run flag set true.")
        valid_config = False

    patch_config = params.get("patch", {})
    if patch_config == {}:
        logger.error("Custom flag is on, but patch section is missing.")
        valid_config = False
    else:
        missing_keys = [key for key in ["start_issue", "end_issue", "patch_dir"] if key not in patch_config]
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

    # every issue would otherwise be generated against the same drop date
    for key in ["drop_date", "end_date"]:
        if params["indicator"].get(key) is not None:
            logger.error("Indicator section must leave this null when patching", key=key)
            valid_config = False

    if valid_config:
        logger.info("Good patch configuration.")
        return True
    logger.info("Bad patch configuration.")
    return False


def patch():
    """
    Run the hospital-admissions indicator for a range of issue dates.

    The range of issue dates is specified in params.json using the following keys:
    - "patch": Only used for patching data
        - "start_issue": str, YYYY-MM-DD format, first issue date
        - "end_issue": str, YYYY-MM-DD format, last issue date
        - "patch_dir": str, directory to write all issues output
    """
    params = read_params()
    logger = get_structured_logger("delphi_claims_hosp.patch", filename=params["common"].get("log_filename"))
    if not good_patch_config(params, logger):
        sys.exit(1)

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

        try:
            run_module(params, logger)
        except NoDropError:
            # one issue with no drop shouldn't take down the rest of the patch
            logger.warning("No drop available for this issue, skipping", issue_date=current_issue.strftime("%Y-%m-%d"))
            rmtree(current_issue_dir)
            rmdir(path.dirname(current_issue_dir))
        current_issue += timedelta(days=1)

    logger.info("Finished patching")


if __name__ == "__main__":
    patch()
