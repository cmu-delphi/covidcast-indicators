from datetime import datetime, timedelta
import json
import subprocess
import os

# Load params
with open('params.json', 'r') as file:
        data = json.load(file)

# Specify the name of the indicator.
#
# This is used only to construct names of the local directories where
# issue data is stored.
SOURCE = "chng"

# Specify where the cache is stored.
#
# The cache should contain all historical data for all signals
# associated with the indicator as of one day before the first
# issue we want to add (AKA `START_DATE`).
cache_dir = "cache/delphi_changehc"
data['archive']['cache_dir'] = cache_dir
run_result_dir = "patch_dec_2022"

# Set first and last issues (inclusive) that we want to diff
START_DATE = datetime(2022, 12, 29)
END_DATE = datetime(2023, 1, 24)

current_date = START_DATE
while current_date <= END_DATE:
    # Convert date to str
    current_date_str = str(current_date.strftime("%Y%m%d"))
    
    print("processing " + current_date_str)

    # Use date str to construct issue directory of the format `issue_YYYYMMDD`
    current_issue_dir = "issue_%s" % current_date_str

    # Set export_dir to be the current issue directory
    data['common']['export_dir'] = "./" + run_result_dir + "/" + current_issue_dir + "/" + SOURCE
    # Save updated params to disk so that the archive differ can access the change
    with open('params.json', 'w') as file:
        json.dump(data, file, indent=4)

    # Run the archive differ.
    #
    # The cache files in cache_dir will be updated when we run
    # the archive differ, so we can keep pointing to the same cache
    # as we progress through all the issues. (Make sure to make a
    # local copy of the cache files in case the diff process goes
    # wrong and you need the original cache again.)
    subprocess.run("env/bin/python -m delphi_utils.archive", shell=True)

    current_date += timedelta(days=1)