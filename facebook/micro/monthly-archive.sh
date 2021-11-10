#!/bin/bash

rm -f *.gz
if [ -z $1 ]; then
    MONTH=`date --date "last month" +"%Y_%m"`
else
    MONTH=$1
fi
echo ${MONTH}
R_MONTH=${MONTH#*_}; R_MONTH=${R_MONTH#0}

perform_rollup_and_post ()
{
    BATCH="cd $1\nls -1 cvid_responses_${MONTH}*.gz"
    sftp -b <(echo -e "${BATCH}") -P 2222 fb-automation@ftp.delphi.cmu.edu 2>/dev/null | \
        grep "^cvid" | \
        awk -F_ -vDIR="$1"  'BEGIN{print "cd " DIR} {key=$3 $4 $5; if (key!=last && last!="") {print record} last=key; record=$0} END{print record}' | \
        sed '/^cvid/ s/^/get /' >fetch.sftp
    sftp -b fetch.sftp -P 2222 fb-automation@ftp.delphi.cmu.edu
    OUT=${MONTH/_/-}
    Rscript ../monthly-files.R ${MONTH%_*} ${R_MONTH} . >${OUT}.csv
    gzip ${OUT}.csv
    sftp -b <(echo -e "cd $1\nput ${OUT}.csv.gz") -P 2222 fb-automation@ftp.delphi.cmu.edu
}

perform_rollup_and_post "fb-public-results"
perform_rollup_and_post "protected-race-ethnicity-data"
