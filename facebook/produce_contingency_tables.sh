#!/bin/sh

printf "Subject: contingency pipeline started" | /usr/sbin/sendmail alerts@alerting_domain

# Fetch recent data and weights
export TODAY=$(/bin/date -u +"%Y-%m-%d")
export CURR_TIME=$(/bin/date -u +"%Hh%Mm%Ss")
export TODAY_DAY=$(/bin/date -u +"%d")

if [ "$TODAY_DAY" = "01" ]; then
    export MONTH=$(/bin/date --date="-1 month" +"%Y-%m")
    export LAST_MONTH=$(/bin/date --date="-2 month" +"%Y-%m")
else
    export MONTH=$(/bin/date -u +"%Y-%m")
    export LAST_MONTH=$(/bin/date --date="-1 month" +"%Y-%m")
fi

export SFTP_DATA_PATH=surveys/raw/
export SFTP_WEIGHTS_PATH=surveys/fb-interchange/cmu_respondent_weights/

export LOCAL_DATA_PATH=$(
    python3 -m delphi_utils get contingency.input_dir
    if [ $? -ne 0 ]; then
            python3 -m delphi_utils get input_dir
    fi
)
export LOCAL_WEIGHTS_PATH=$(
    python3 -m delphi_utils get contingency.weights_in_dir
    if [ $? -ne 0 ]; then
            python3 -m delphi_utils get weights_in_dir
    fi
)

export SFTP_DOWNLOAD_COMMANDS="get ${SFTP_DATA_PATH}/${MONTH}*.csv ${LOCAL_DATA_PATH}\nget ${SFTP_WEIGHTS_PATH}/${MONTH}*.csv ${LOCAL_WEIGHTS_PATH}\nget ${SFTP_DATA_PATH}/${LAST_MONTH}*.csv ${LOCAL_DATA_PATH}/\nget ${SFTP_WEIGHTS_PATH}/${LAST_MONTH}*.csv ${LOCAL_WEIGHTS_PATH}/\n"
printf "${SFTP_DOWNLOAD_COMMANDS}" | sftp -b - -P 2222 uploader@ftp_site

if [ $? -ne 0 ]; then
    printf "Subject: !!problem downloading data and weights for contingency pipeline!!" | /usr/sbin/sendmail alerts@alerting_domain
    exit
fi


# Run pipeline. If any step fails, stops and sends message.
# ** Does not pull and reinstall package when updates are made since installation needs sudo access.
export AGG_TYPE=$1 # Uses first arg to shell script ("month" or "week")
export OUT_DIR=contingency_receiving
export ROLLUP_DIR=${AGG_TYPE}ly-rollup

export SFTP_UPLOAD_PATH=~/delphi-web/surveys/
export MONTH_NO_DASH=$(/bin/date -u +"%Y%m")
export LASTMONTH_NO_DASH=$(/bin/date --date="-1 month" +"%Y%m")

rm -f $OUT_DIR/*
python3 -m delphi_utils set contingency.aggregate_range $AGG_TYPE
python3 -m delphi_utils set contingency.end_date $TODAY
python3 -m delphi_utils set contingency.debug false 
python3 -m delphi_utils set contingency.parallel true
Rscript contingency_tables.R && Rscript check_sample_sizes.R $OUT_DIR && Rscript contingency-combine.R $OUT_DIR $ROLLUP_DIR && Rscript check_sample_sizes.R $ROLLUP_DIR

if [ $? -eq 0 ]; then
    printf "Subject: contingency pipeline run complete" | /usr/sbin/sendmail alerts@alerting_domain
else
    printf "Subject: !!contingency pipeline encountered a problem!!" | /usr/sbin/sendmail alerts@alerting_domain
    exit
fi


# Upload 
scp ${OUT_DIR}/${MONTH_NO_DASH}*.csv.gz auto_uploader@awps:${SFTP_UPLOAD_PATH}/${AGG_TYPE}ly
scp ${OUT_DIR}/${LASTMONTH_NO_DASH}*.csv.gz auto_uploader@awps:${SFTP_UPLOAD_PATH}/${AGG_TYPE}ly
scp ${ROLLUP_DIR}/${AGG_TYPE}ly*.csv.gz auto_uploader@awps:${SFTP_UPLOAD_PATH}/${ROLLUP_DIR}

if [ $? -eq 0 ]; then
        printf "Subject: contingency tables uploaded to publishing\n\nThey should appear in 90 min or less" | /usr/sbin/sendmail alerts@alerting_domain
else
        printf "Subject: !!contingency table upload encountered a problem!!" | /usr/sbin/sendmail alerts@alerting_domain
    exit
fi

# Tidy and save archive
export TIDY_DIR=tidy-${AGG_TYPE}-${TODAY}-${CURR_TIME}
mkdir $TIDY_DIR
cp params.json $TIDY_DIR
cp contingency_input.txt $TIDY_DIR
cp -r $OUT_DIR $TIDY_DIR
cp -r $ROLLUP_DIR $TIDY_DIR
tar -czf ${TIDY_DIR}.tgz $TIDY_DIR
rm -rf $TIDY_DIR

rm -f $OUT_DIR/*
