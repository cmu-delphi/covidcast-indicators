#!/usr/bin/env bash
#
# Run the Safegraph Patterns indicator
#

#set -eo pipefail

# Vars.
# Date of first monday 8 weeks in the past. Taste to suit!
start_day=$(date --date 'monday-56 days' +"%Y%m%d")
today=$(date +"%Y%m%d")

# Purge the receiving directory.
echo "Purging ./receiving..."
rm -f ./receiving/*

# Run the indicator code.
echo "Running the indicator..."
env/bin/python -m delphi_safegraph_patterns

# Copy the files to the ingestion directory.
# The unwieldy one-liner does the following:
# - Pipe a list of files into awk.
# - awk prints the files that are inclusive of ${start_day} to ${today}.
# - Pipe that list into xargs and copy those files to the receiving dir.
echo "Copying files to the ingestion directory..."
cd ./receiving || exit
ls -1 *.csv | awk '$0>=from && $0<=to' from="${start_day}" to="${today}" \
  | xargs cp -t /common/covidcast/receiving/safegraph
