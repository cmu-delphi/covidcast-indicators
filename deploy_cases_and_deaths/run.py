#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 29 20:59:58 2020
"""
import re
import argparse
from argparse import RawTextHelpFormatter
from itertools import product
from datetime import date, timedelta, datetime

import covidcast


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

def combine_usafacts_and_jhu(signal, geo, _date):
    """
    Add rows for PR from JHU signals to USA-FACTS signals
    """
    usafacts_df = covidcast.signal("usa-facts", signal, _date, _date, geo)
    jhu_df = covidcast.signal("jhu-csse", signal, _date, _date, geo)
    # State level
    if geo == 'state':
        combined_df = usafacts_df.append(jhu_df[jhu_df["geo_value"] == 'or'])        
    # County level
    elif geo == 'county':
        combined_df = usafacts_df.append(jhu_df[jhu_df["geo_value"] == '72000'])   
    # For MSA and HRR level, they are the same
    else:
        combined_df = usafacts_df
    return combined_df

def run():    
    for metric, geo_res, sensor, smoother, _date in product(
                METRICS, GEO_RESOLUTIONS, SENSORS, SMOOTH_TYPES, date_list):
        if smoother == "7dav":
            signal = "_".join([metric, smoother, sensor])
        else:
            signal = "_".join([metric, sensor])
        df = combine_usafacts_and_jhu(signal, geo_res, _date)
        df.to_csv("./receiving/%s_%s_%s.csv"%(_date, geo_res, signal))
       


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
if args.date_range == 'new':
    # only create combined file for the newest update (usually for yesterday)    
    date_list = [yesterday]
elif args.date_range == 'all':
    # create combined files for all of the historical reports
    delta = yesterday - date(2020, 2, 20)
    date_list = [date(2020, 2, 20) + timedelta(days=i) for i in range(delta.days + 1)]
else:
    pattern = re.compile('^\d{8}-\d{8}$')
    match_res = re.findall(pattern, args.date_range)
    if match_res[0] == None:
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
        delta = date2 - date1
        date_list = [date1 + timedelta(days=i) for i in range(delta.days + 1)]

run()                            
        

    
 