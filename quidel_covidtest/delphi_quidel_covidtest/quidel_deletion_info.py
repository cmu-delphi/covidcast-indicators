#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb  5 23:12:12 2022

@author: jingjingtang
"""
from os.path import join
import os
from datetime import datetime, timedelta
import boto3

import numpy as np
import pandas as pd

from delphi_utils import (
    add_prefix,
    create_export_csv,
    get_structured_logger,
    read_params
)
from .pull import *
from .constants import * 
from .geo_maps import geo_map
from .generate_sensor import * 


AGE_GROUPS = [
    "total",
    # "age_0_4",
    # "age_5_17",
    # "age_18_49",
    # "age_50_64",
    # "age_65plus",
    # "age_0_17",
]


params = read_params()

logger = get_structured_logger(
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))

aws_access_key_id = params["indicator"]["aws_credentials"]["aws_access_key_id"]
aws_secret_access_key = params["indicator"]["aws_credentials"]["aws_secret_access_key"]
bucket_name = params["indicator"]["bucket_name"]
s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key)
bucket = s3.Bucket(bucket_name)


issue_start_date = datetime(2020, 7, 29)
issue_end_date = datetime(2022, 2, 5)
n_days = (issue_end_date - issue_start_date).days + 1

sensors = add_prefix(SENSORS,
                         wip_signal=params["indicator"]["wip_signal"],
                         prefix="wip_")
smoothers = get_smooth_info(sensors, SMOOTHERS)

######################################################################
#####################initialization###################################
######################################################################
start_date, end_date = datetime(2020, 5, 26), datetime(2020, 7, 28)
# Get new data from s3
df, time_flag = get_from_s3(start_date, end_date, bucket, logger)

# Fix some of the fipcodes that are 9 digit instead of 5 digit
df = fix_zipcode(df)
# Create a column CanonicalDate according to StarageDate and TestDate
df = fix_date(df, logger)
    
# Compute overallPositive
overall_pos = df[df["OverallResult"] == "positive"].groupby(
    by=["timestamp", "zip"],
    as_index=False)['OverallResult'].count()
overall_pos["positiveTest_total"] = overall_pos["OverallResult"]
overall_pos.drop(labels="OverallResult", axis="columns", inplace=True)

# Compute overallTotal
overall_total = df.groupby(
    by=["timestamp", "zip"],
    as_index=False)['OverallResult'].count()
overall_total["totalTest_total"] = overall_total["OverallResult"]
overall_total.drop(labels="OverallResult", axis="columns", inplace=True)

# Compute numUniqueDevices
numUniqueDevices = df.groupby(
    by=["timestamp", "zip"],
    as_index=False)["SofiaSerNum"].agg({"SofiaSerNum": "nunique"}).rename(
        columns={"SofiaSerNum": "numUniqueDevices_total"}
        )

df_merged = overall_total.merge(
    numUniqueDevices, on=["timestamp", "zip"], how="left"
    ).merge(
        overall_pos, on=["timestamp", "zip"], how="left"
        ).fillna(0).drop_duplicates()


df_combined = df_merged.copy()
df_combined["zip"] = df_combined["zip"].astype(int)


for issue_date in [issue_start_date + timedelta(days=x) for x in range(n_days)]:
    print(issue_date)
    
    start_date, end_date = issue_date, issue_date

    # Get new data from s3
    df, time_flag = get_from_s3(start_date, end_date, bucket, logger)

    # Fix some of the fipcodes that are 9 digit instead of 5 digit
    df = fix_zipcode(df)
    # Create a column CanonicalDate according to StarageDate and TestDate
    df = fix_date(df, logger)
    
    # Compute overallPositive
    overall_pos = df[df["OverallResult"] == "positive"].groupby(
        by=["timestamp", "zip"],
        as_index=False)['OverallResult'].count()
    overall_pos["positiveTest_total"] = overall_pos["OverallResult"]
    overall_pos.drop(labels="OverallResult", axis="columns", inplace=True)

    # Compute overallTotal
    overall_total = df.groupby(
        by=["timestamp", "zip"],
        as_index=False)['OverallResult'].count()
    overall_total["totalTest_total"] = overall_total["OverallResult"]
    overall_total.drop(labels="OverallResult", axis="columns", inplace=True)

    # Compute numUniqueDevices
    numUniqueDevices = df.groupby(
        by=["timestamp", "zip"],
        as_index=False)["SofiaSerNum"].agg({"SofiaSerNum": "nunique"}).rename(
            columns={"SofiaSerNum": "numUniqueDevices_total"}
        )

    df_merged = overall_total.merge(
        numUniqueDevices, on=["timestamp", "zip"], how="left"
        ).merge(
        overall_pos, on=["timestamp", "zip"], how="left"
        ).fillna(0).drop_duplicates()
    df_merged["zip"] = df_merged["zip"].astype(int)
            
    df_combined = df_combined.append(df_merged).groupby(["timestamp", "zip"]).sum().reset_index()
    
    
    first_date, last_date = df_combined["timestamp"].min(), df_combined["timestamp"].max()
    export_end_date = issue_date - timedelta(days=5)
    export_start_date = export_end_date - timedelta(days=50)
    
    data = df_combined.copy()
    
    
    export_dir = "receiving_%s"%issue_date.date()
    os.system("mkdir %s"%export_dir)
        
    for geo_res in NONPARENT_GEO_RESOLUTIONS:
        geo_data, res_key = geo_map(geo_res, data)
        geo_groups = geo_data.groupby(res_key)
        for agegroup in AGE_GROUPS:
            for sensor in sensors:
                if agegroup == "total":
                    sensor_name = sensor
                else:
                    sensor_name = "_".join([sensor, agegroup])
                logger.info("Generating signal and exporting to CSV",
                            geo_res=geo_res,
                            sensor=sensor_name)
                state_df = generate_sensor_for_nonparent_geo(
                    geo_groups, res_key, smooth=smoothers[sensor][1],
                    device=smoothers[sensor][0], first_date=first_date,
                    last_date=last_date, suffix=agegroup)
                dates = create_export_csv(
                    state_df,
                    geo_res=geo_res,
                    sensor=sensor_name,
                    export_dir=export_dir,
                    start_date=export_start_date,
                    end_date=export_end_date)

    assert geo_res == "state" # Make sure geo_groups is for state level
    # County/HRR/MSA level
    for geo_res in PARENT_GEO_RESOLUTIONS:
        geo_data, res_key = geo_map(geo_res, data)
        for agegroup in AGE_GROUPS:
            for sensor in sensors:
                if agegroup == "total":
                    sensor_name = sensor
                else:
                    sensor_name = "_".join([sensor, agegroup])
                logger.info("Generating signal and exporting to CSV",
                            geo_res=geo_res,
                            sensor=sensor_name)
                res_df = generate_sensor_for_parent_geo(
                    geo_groups, geo_data, res_key, smooth=smoothers[sensor][1],
                    device=smoothers[sensor][0], first_date=first_date,
                    last_date=last_date, suffix=agegroup)
                dates = create_export_csv(res_df, geo_res=geo_res,
                                          sensor=sensor_name, export_dir=export_dir,
                                          start_date=export_start_date,
                                          end_date=export_end_date,
                                          remove_null_samples=True)


######################################################################
###################################sanity_check
######################################################################
import covidcast
quidel_df = covidcast.signal('quidel', 'covid_ag_smoothed_pct_positive', 
                             start_day=datetime(2021, 10, 4), 
                             end_day=datetime(2021, 10, 4), 
                             geo_type='county', as_of=datetime(2021, 11, 30))


######################################################################
###################################Combine output
######################################################################
pdList = []
for issue_date in [issue_start_date + timedelta(days=x) for x in range(n_days)]:
    print(issue_date)
    export_dir = "receiving_%s"%issue_date.date()
    for fn in os.listdir(export_dir):
        time_value = datetime.strptime(fn.split("_")[0], "%Y%m%d")
        geo_type = fn.split("_")[1]
        df = pd.read_csv(export_dir+"/"+fn)
        df["issue"] = issue_date
        df["time_value"] = time_value
        df["geo_type"] = geo_type
        df["signal"] = 'covid_ag_smoothed_pct_positive'
        df["source"] = 'quidel'
        df.rename({"val": "value", "se": "stderr"}, axis=1, inplace=True)
        pdList.append(df)
final = pd.concat(pdList)
final.to_csv("~/Downloads/quidel_deletion.csv", index=False)
        
        

