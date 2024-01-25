from datetime import datetime, timedelta
import json
import subprocess
from os import makedirs
from delphi_utils import read_params
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

    current_date = START_DATE
    while current_date <= END_DATE:
        date_str = str(current_date.strftime("%Y%m%d"))
        print(date_str)

        issue_dir = "issue_%s" % date_str
        makedirs(f"{PATCH_DIR}/{issue_dir}/{SOURCE}", exist_ok=True) #create issue & source dir

        params['common']['epidata']['as_of'] = date_str
        params['common']['export_dir'] = f"{PATCH_DIR}/{issue_dir}/{SOURCE}"

        run_module(params)
        print(f"completed run for issue_{issue_dir}")
        current_date += timedelta(days=1)