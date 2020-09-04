#!/usr/bin/env bash
#
# Run the Safegraph indicator
#

#set -eo pipefail

# Purge the receiving directory.
echo "Purging ./receiving..."
rm -f ./receiving/*

# Run the indicator code.
echo "Running the indicator..."
env/bin/python -m delphi_safegraph

# Copy the files to the ingestion directory.
echo "Copying files to the ingestion directory..."
# Hack to make cp care less about missing recent files since we don't always have them.
cp $(date +"receiving/%Y%m*") /common/covidcast/receiving/safegraph 2>/dev/null
cp $(date --date='-1 month' +"receiving/%Y%m*") /common/covidcast/receiving/safegraph
