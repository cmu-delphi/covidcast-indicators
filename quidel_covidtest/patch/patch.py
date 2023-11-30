from delphi_utils import read_params
from os import makedirs
from datetime import datetime, timedelta

#Manually set before running Script
from delphi_quidel_covidtest.run import run_module

START_DATE = datetime(2023, 4, 2)#20230220
END_DATE = datetime(2023, 4, 10)

DELTA = 45

PATCH_DIR = ""
INDICATOR_PREFIX = "quidel"

# Common parameter setup
params = read_params()
params["common"]["log_filename"] = f"{PATCH_DIR}/{INDICATOR_PREFIX}.log"
params["indicator"]["input_cache_dir"] = f"{PATCH_DIR}/input_cache"
params["indicator"]["generate_backfill_files"] = False

#Make common patch directories and create log file
makedirs(PATCH_DIR, exist_ok=True)
makedirs(params['indicator']['input_cache_dir'], exist_ok=True)
with open(params["common"]["log_filename"], "w") as log_file:
    log_file.write(f"Starting patch script for {INDICATOR_PREFIX}")


#Loop through each issue date
current_issue = START_DATE
while current_issue <= END_DATE:
    issue_name = "issue_" + str(current_issue.strftime("%Y%m%d"))
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

    print(f"completed run for issue_{issue_name}")
    current_issue += timedelta(days=1)