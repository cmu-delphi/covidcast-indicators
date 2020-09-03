#!/usr/bin/env bash
#
# Run the Safegraph indicator
#

set -eo pipefail

# Purge the receiving directory.
echo "Purging ./receiving..."
rm -f ./receiving/*

# Run the indicator code.
echo "Running the indicator..."
env/bin/python -m delphi_safegraph

# Copy the files to the ingestion directory.
#scp $(date +"receiving/%Y%m*") delphi.midas.cs.cmu.edu:/common/covidcast/receiving/safegraph/
#scp $(date --date='-1 month' +"receiving/%Y%m*") delphi.midas.cs.cmu.edu:/common/covidcast/receiving/safegraph/
echo "Copying files to the ingestion directory..."
cp $(date +"receiving/%Y%m*") ./test-output/safegraph 2>/dev/null # Hack to make cp care less about missing files.
cp $(date --date='-1 month' +"receiving/%Y%m*") ./test-output/safegraph
