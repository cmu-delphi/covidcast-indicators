#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script produces a combined signal for jhu-csse and usa-facts.
This signal is only used for visualization. 
It includes all of the information in usa-facts and Puerto Rico only from jhu-csse.
"""
import re
import argparse
from argparse import RawTextHelpFormatter
from itertools import product
from datetime import date, timedelta, datetime

import pandas as pd

from delphi_utils import create_export_csv
import covidcast

export_start_date = date(2020, 4, 1)
METRICS = [
    "confirmed",
    "deaths",
]
SMOOTH_TYPES = [
    "",
    "7dav",
]
SENSORS = [
    "incidence_num",
    "cumulative_num",
    "incidence_prop",
    "cumulative_prop",
]
GEO_RESOLUTIONS = [
    "county",
    "state",
    "msa",
    "hrr",
]

def combine_usafacts_and_jhu(signal, geo, date_range):
    """
    Add rows for PR from JHU signals to USA-FACTS signals
    """
    usafacts_df = covidcast.signal("usa-facts", signal, date_range[0], date_range[1], geo)
    jhu_df = covidcast.signal("jhu-csse", signal, date_range[0], date_range[1], geo)
    # State level
    if geo == 'state':
        combined_df = usafacts_df.append(jhu_df[jhu_df["geo_value"] == 'pr'])        
    # County level
    elif geo == 'county':
        combined_df = usafacts_df.append(jhu_df[jhu_df["geo_value"] == '72000'])   
    # For MSA and HRR level, they are the same
    else:
        combined_df = usafacts_df
    
    combined_df = combined_df.drop(["direction"], axis = 1)
    combined_df = combined_df.rename({"time_value": "timestamp", 
                                      "geo_value": "geo_id",
                                      "value": "val",
                                      "stderr": "se"}, 
                                     axis = 1)
    return combined_df

def run(date_range):  
    
    export_dir = "./receiving"
    
    for metric, geo_res, sensor, smoother in product(
                METRICS, GEO_RESOLUTIONS, SENSORS, SMOOTH_TYPES):
        if smoother == "7dav":
            sensor_name = "_".join([smoother, sensor])
        else:
            sensor_name = sensor
        signal = "_".join([metric, sensor_name])
        df = combine_usafacts_and_jhu(signal, geo_res, date_range)
        
        create_export_csv(
            df,
            export_dir=export_dir,
            start_date=pd.to_datetime(export_start_date),
            metric=metric,
            geo_res=geo_res,
            sensor=sensor_name,
        )
       


parser = argparse.ArgumentParser(description='This script produces a combined signal for jhu-csse and usa-facts. \n \
This signal is only used for visualization. It includes all of the information in usa-facts and Puerto Rico from jhu-csse. \n \
\t [--date_range] Enter the date range for the combined signal. Can choose from (new, all, yyyymmdd-yyyymmdd). \n \
Example: \n \
\t$python run.py --date_range 20200501-20200628 \n \
Please see the documentation for more details.',formatter_class=RawTextHelpFormatter)

#INPUT ARGUMENTS
grpInput = parser.add_argument_group('Input')
grpInput.add_argument('--date_range', type=str, dest='date_range',default= "new", help='Enter the date range for the combined signal. Can choose from (new, all, yyyymmdd-yyyymmdd).')

args = parser.parse_args()

yesterday = date.today() - timedelta(days=1)
date_list = None
if args.date_range == 'new':
    # only create combined file for the newest update (usually for yesterday)    
    date_range = [yesterday, yesterday]
elif args.date_range == 'all':
    # create combined files for all of the historical reports
    date_range = [export_start_date, yesterday]
else:
    pattern = re.compile('^\d{8}-\d{8}$')
    match_res = re.findall(pattern, args.date_range)
    if len(match_res) == 0:
        raise ValueError("Invalid input. Please choose from (new, all, yyyymmdd-yyyymmdd).")
    else:
        try:
            date1 = datetime.strptime(args.date_range[:8], '%Y%m%d').date()
        except ValueError:
            raise ValueError("Invalid input. Please check the first date.")
        try:
            date2 = datetime.strptime(args.date_range[-8:], '%Y%m%d').date()
        except ValueError:
            raise ValueError("Invalid input. Please check the second date.")
            
        #The the valid start date
        if date1 < export_start_date:
            date1 = export_start_date            
        date_range = [date1, date2]

if date_range:
    run(date_range)   
                         
        

    
 
