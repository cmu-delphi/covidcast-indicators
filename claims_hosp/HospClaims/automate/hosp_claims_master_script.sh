#!/bin/sh
set -o errexit
set -o pipefail

BASE="/home/indicators/runtime/claims_hosp/HospClaims"

AUTO_DIR="$BASE/automate"
HOSP_CLAIMS_PKG_DIR="/home/indicators/runtime/claims_hosp"
CLAIMS_DIR="$BASE/claims_data"
GEO_DIR="/common/covidcast/covid-19/geographical_scope"
CURRENT_dmY=$(date '+%d%m%Y')
CURRENT_Ymd=$(date '+%Y%m%d')
CURRENT_YmdHM=$(date '+%Y%m%d_%H%M')
RECEIVING_DIR="$BASE/receiving/results_$CURRENT_YmdHM"
RECEIVING_SE_DIR="$BASE/receiving/results_se_$CURRENT_YmdHM"

# pull latest data
echo "downloading drops"
cd "$AUTO_DIR" || exit
python3 download_claims_ftp_files.py "$CLAIMS_DIR"

# aggregate data
echo "aggregating drops"
python3 agg_claims_drops.py "$CLAIMS_DIR"

# find the latest files (these have timestamps)
echo "finding today's latest claims drop"
claims_file=$(python3 get_latest_claims_name.py "$CLAIMS_DIR")

# make receiving directories
mkdir "$RECEIVING_DIR"

# generate the sensor
cd "$HOSP_CLAIMS_PKG_DIR" || exit

source env/bin/activate

python $AUTO_DIR/update_json.py \
  "$claims_file" \
  "$GEO_DIR" \
  "$HOSP_PKG_DIR" \
  "$RECEIVING_DIR"

python -m delphi_claims_hosp

deactivate

sanity_check() {
  geo=$1
  cd "$AUTO_DIR" || exit
  python3 sanity_checks.py "$RECEIVING_DIR" "$geo"
}

echo "running sanity checks"
sanity_check state
sanity_check msa
sanity_check hrr
sanity_check county

# plot states without se
cd "$AUTO_DIR" || exit
python3 sanity_checks.py "$RECEIVING_DIR" state -p

# upload files to covidcast
python3 ftp_to_covidcast.py "$RECEIVING_DIR"

# delete raw data
rm "$CLAIMS_DIR"/*.csv.gz

# delete signal files
rm -r "$RECEIVING_DIR" 
