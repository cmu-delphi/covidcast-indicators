from delphi_utils import read_params, get_structured_logger
from os import makedirs
from datetime import datetime, timedelta
import argparse
from delphi_quidel_covidtest.run import run_module

if __name__ == "__main__":

    # Read parameters from cmd line or params.json
    parser = argparse.ArgumentParser(description='Run patch script for changehc')
    parser.add_argument('--patch_dir', default=None, help='Patch directory')
    parser.add_argument('--start_date', default=None, help='Start date in yyyy-mm-dd format')
    parser.add_argument('--end_date', default=None, help='End date in yyyy-mm-dd format')
    args = parser.parse_args()

    params = read_params()

    PATCH_DIR = args.patch_dir if args.patch_dir else params["patch"]["patch_dir"]  # Directory for patches
    START_DATE = datetime.strptime(args.start_date if args.start_date else params["patch"]["start_date"], "%Y-%m-%d")  # Start date (yyyy-mm-dd)
    END_DATE = datetime.strptime(args.end_date if args.end_date else params["patch"]["end_date"], "%Y-%m-%d")  # End date
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

    logger = get_structured_logger(
        __file__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))
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

        logger.info(f"completed indicator run for {issue_name}")

        current_issue += timedelta(days=1)