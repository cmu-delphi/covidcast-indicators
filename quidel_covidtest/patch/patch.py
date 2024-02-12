from delphi_utils import read_params, get_structured_logger, params_run
from os import makedirs
import sys
from datetime import datetime, timedelta
from delphi_quidel_covidtest.run import run_module

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
    DELTA = 45
    INDICATOR_PREFIX = "quidel"

    params["common"]["log_filename"] = f"{PATCH_DIR}/{INDICATOR_PREFIX}.log"
    params["indicator"]["input_cache_dir"] = f"{PATCH_DIR}/input_cache"
    params["indicator"]["generate_backfill_files"] = False

    if "archive" in params:
        params.pop("archive")

    #Make common patch directories and create log file
    makedirs(PATCH_DIR, exist_ok=True)
    makedirs(params['indicator']['input_cache_dir'], exist_ok=True)

    logger.info(f"Starting patch script for {INDICATOR_PREFIX} from {START_DATE} to {END_DATE}")

    #Loop through each issue date
    current_issue = START_DATE
    while current_issue <= END_DATE:
        issue_name = "issue_" + str(current_issue.strftime("%Y%m%d"))
        logger.info(f"Starting indicator run for {issue_name}")

        params["common"]["export_dir"] = f"{PATCH_DIR}/{issue_name}/{INDICATOR_PREFIX}"
        
        end_date = str(current_issue.strftime("%Y-%m-%d"))
        start_date = current_issue - timedelta(days=DELTA)
        start_date = str(start_date.strftime("%Y-%m-%d"))
        
        params["indicator"]["export_start_date"] = start_date
        params["indicator"]["pull_start_date"] = start_date 
        
        params["indicator"]["export_end_date"] = end_date
        params["indicator"]["pull_end_date"] = end_date
        
        makedirs(params["common"]["export_dir"], exist_ok=True)

        run_module(params)

        logger.info(f"Completed indicator run for {issue_name}")

        current_issue += timedelta(days=1)