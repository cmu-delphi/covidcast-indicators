from datetime import datetime, timedelta
import json
import subprocess
from os import makedirs
from delphi_utils import read_params, get_structured_logger, params_run
import sys

from delphi_changehc.run import run_module


if __name__ == "__main__":
    # Read parameters from cmd line or params.json. cmd line configuration takes precedence over params.json
    # Example cmd: python3 patch.py set patch.patch_dir patch_jan_13_14 patch.start_date 2024-01-13 patch.end_date 2024-01-14
    params = read_params()
    json_patch_conf = "patch" in params and "start_date" in params["patch"] and "end_date" in params["patch"]
    cmd_patch_conf = len(sys.argv) > 1 and sys.argv[1] == "set"

    logger = get_structured_logger(
        __file__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))

    if json_patch_conf == False and cmd_patch_conf == False:
        logger.error("Please add 'start_date' and 'end_date' to params.json under 'patch' or specify in cmd line.")
        sys.exit(1)
    elif cmd_patch_conf == True:
        params_run() #set start_date and end_date from cmd line
        params = read_params()

    START_DATE = datetime.strptime(params["patch"]["start_date"], "%Y-%m-%d")  # Start date (yyyy-mm-dd)
    END_DATE = datetime.strptime(params["patch"]["end_date"], "%Y-%m-%d")  # End date
    PATCH_DIR = params["patch"].get("patch_dir", f"patch_{START_DATE}_{END_DATE}")  # Directory for issues 
    SOURCE = "chng"  # Source name

    # Ensure patch directory exists
    makedirs(PATCH_DIR, exist_ok=True)

    # Set log filename
    params["common"]["log_filename"] = f"{PATCH_DIR}/{SOURCE}.log"
    params["indicator"]["generate_backfill_files"] = False

    # Remove 'archive' key from params if it exists
    if "archive" in params:
        params.pop("archive")

    logger.info(f"Starting patch script for {SOURCE} from {START_DATE} to {END_DATE}")

    current_date = START_DATE
    while current_date <= END_DATE:
        date_str = str(current_date.strftime("%Y%m%d"))
        issue_name = f"issue_{date_str}"
        logger.info(f"Starting indicator run for {issue_name}")

        drop_date = current_date - timedelta(days=1)
        params['indicator']['drop_date'] = str(drop_date.strftime("%Y-%m-%d"))
        params['common']['export_dir'] = f"{PATCH_DIR}/{issue_name}/{SOURCE}"
        makedirs(params["common"]["export_dir"], exist_ok=True)

        run_module(params)
        logger.info(f"Completed indicator run for {issue_name}")

        current_date += timedelta(days=1)
