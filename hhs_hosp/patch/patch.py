from datetime import datetime, timedelta
import json
import subprocess
from os import makedirs
from delphi_utils import read_params, get_structured_logger
from delphi_hhs.run import run_module
import argparse

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
    SOURCE = "hhs"

    makedirs(PATCH_DIR, exist_ok=True)

    if "archive" in params:
        params.pop("archive")

    logger = get_structured_logger(
        __file__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))
    logger.info(f"Starting patch script for {SOURCE} from {START_DATE} to {END_DATE}")


    current_date = START_DATE
    while current_date <= END_DATE:
        date_str = str(current_date.strftime("%Y%m%d"))
        issue_name = f"issue_{date_str}"
        logger.info(f"Starting indicator run for {issue_name}")

        params['common']['epidata']['as_of'] = date_str
        params['common']['export_dir'] = f"{PATCH_DIR}/{issue_name}/{SOURCE}"
        makedirs(params["common"]["export_dir"], exist_ok=True) #create issue & source dir

        run_module(params)
        logger.info(f"Completed indicator run for {issue_name}")
        current_date += timedelta(days=1)