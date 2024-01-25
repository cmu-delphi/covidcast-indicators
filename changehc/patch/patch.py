from datetime import datetime, timedelta
import json
import subprocess
from os import makedirs
from delphi_utils import read_params
import argparse

from delphi_changehc.run import run_module


if __name__ == "__main__":
        # Read parameters from cmd line or params.json
        parser = argparse.ArgumentParser(description='Run patch script for changehc')
        parser.add_argument('--patch_dir', default=None, help='Patch directory')
        parser.add_argument('--start_date', default=None, help='Start date in yyyy-mm-dd format')
        parser.add_argument('--end_date', default=None, help='End date in yyyy-mm-dd format')
        args = parser.parse_args()

        params = read_params()
        

        # Constants
        SOURCE = "chng"  # Source name
        PATCH_DIR = args.patch_dir if args.patch_dir else params["patch"]["patch_dir"]  # Directory for patches
        START_DATE = datetime.strptime(args.start_date if args.start_date else params["patch"]["start_date"], "%Y-%m-%d")  # Start date (yyyy-mm-dd)
        END_DATE = datetime.strptime(args.end_date if args.end_date else params["patch"]["end_date"], "%Y-%m-%d")  # End date

        # Ensure patch directory exists
        makedirs(PATCH_DIR, exist_ok=True)

        # Set log filename
        params["common"]["log_filename"] = f"{PATCH_DIR}/{SOURCE}.log"

        # Remove 'archive' key from params if it exists
        if "archive" in params:
                params.pop("archive")

        current_date = START_DATE
        while current_date <= END_DATE:
                date_str = str(current_date.strftime("%Y%m%d"))
                print(date_str)

                issue_dir = "issue_%s" % date_str
                makedirs(f"{PATCH_DIR}/{issue_dir}/{SOURCE}", exist_ok=True) #create issue & source dir

                drop_date = current_date - timedelta(days=1)
                params['indicator']['drop_date'] = str(drop_date.strftime("%Y-%m-%d"))
                params['common']['export_dir'] = f"{PATCH_DIR}/{issue_dir}/{SOURCE}"
                makedirs(params["common"]["export_dir"], exist_ok=True)
                
                run_module(params)

                print(f"completed run for issue_{issue_dir}")

                current_date += timedelta(days=1)
