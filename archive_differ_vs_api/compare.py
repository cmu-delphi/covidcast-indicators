'''
This widget compares what's on our archive differ S3 bucket with what's on the latest table from Epidata API by sources.
Specifically, it pulls each csv file (of a specified source) on the archive differ s3 bucket on aws.
Each of these csv are labeled by the source, time_value, geo, and signal.
Using these labels, it pulls the equivalent, supposedly matching, latest data from the Epidata API.
It then compares the two resulting dataframes and output the row numbers on each side, as well as the number of rows that are different.
Result is written to a json-like file.

Usage:
- Add AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_SOURCE and API_KEY as environment variable.
    + S3_SOURCE must be a key in SOURCES dictionary.
- Run the script: `python compare.py`
- Output is written to a json-like file named after the S3_SOURCE.
    + Note that the output isn't a valid json file, but a series of json objects separated by commas.

Note:
The resulting comparisons has taken into account:
- floating point precision differences.
- order of rows.
- weekly/daily time types.
- missingness (NA values).

'''
import boto3
import regex
from datetime import datetime
import pandas as pd
import numpy as np
import covidcast
from delphi_epidata import Epidata
import warnings
import json
import os
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=RuntimeWarning)

# Get these from environment variables
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
API_KEY = os.getenv("API_KEY")
S3_SOURCE = os.getenv("S3_SOURCE")

covidcast.use_api_key(API_KEY)
Epidata.debug = True
Epidata.auth = ("epidata", API_KEY)

client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
bucket = s3.Bucket(BUCKET_NAME)

SOURCES = {
    'delphi_hhs_hosp': 'hhs',
    'delphi_changehc': 'chng',
    'delphi_dsew_community_profile': 'dsew-cpr',
    'jhu': 'jhu-csse',
    'nchs_mortality': 'nchs-mortality',
    'quidel': 'quidel',
    'usafacts': 'usa-facts',
}

WEEKLY_SOURCES = {'nchs_mortality': 'nchs-mortality'}

def dump_json(data):
    f = open(f'{S3_SOURCE}.json', 'a')
    json.dump(data, f)
    f.write(",\n")
    f.close()

if S3_SOURCE in WEEKLY_SOURCES:
    api_time_type = "week"
else:
    api_time_type = "day"

count = 0
full_file_dif_potential = False
result_df = pd.DataFrame()
# for obj in bucket.objects.all(): #for each file in bucket
for obj in bucket.objects.filter(Prefix=S3_SOURCE):
    count +=1
    print(f'{count}', end="", flush=True)
    # print(f'{count} {obj.key} ... ', end="", flush=True)

    prefixes = obj.key.split("/")
    source_s3 = prefixes[0]
    source_api = SOURCES[source_s3]

    # s3 file name not valid
    csvname_s3 = prefixes[1]
    if len(csvname_s3)==0:
        # print("file has no name")
        row = {"file_name":obj.key, "source":source_api, "skip":True, "reason": "file has no name"}
        dump_json(row)
        continue

    # time_value
    if source_s3 in WEEKLY_SOURCES:
        time_value_s3 = csvname_s3.split("_")[1]
    else:
        time_value_s3 = csvname_s3.split("_")[0]
    if time_value_s3[:2]!="20": 
        # print("file has non-standardized naming")
        row = {"file_name":obj.key, "source":source_api, "skip":True, "reason": "file has non-standardized naming"}
        dump_json(row)
        continue

    # geo
    if source_s3 in WEEKLY_SOURCES:
        geo_s3 = csvname_s3.split("_")[2]
    else:
        geo_s3 = csvname_s3.split("_")[1]
    geo_is_str = geo_s3 in ["nation", "state"]

    # signal
    if source_s3 in WEEKLY_SOURCES:
        signal_s3 = regex.search(r"(?<=\w+_\d+_\w+_)\w+(?=\.csv$)", csvname_s3).group(0)
    else:
        signal_s3 = regex.search(r"(?<=\d+_\w+_)\w+(?=\.csv$)", csvname_s3).group(0)

    if 'wip' in signal_s3:
        row = {"file_name":obj.key, "source":source_api, "skip":True, "reason": f"wip in signal name"}
        dump_json(row)
        continue
    else:
        signal_api = signal_s3

    #print (csvname_s3, source_s3, time_value_s3, geo_s3, signal_s3)

    #S3
    response = client.get_object(Bucket=BUCKET_NAME, Key=obj.key)

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        df_s3 = pd.read_csv(response.get("Body"))
        # print(df_s3)
        df_s3['sample_size'] = df_s3['sample_size'].map('{:,.6f}'.format)
        df_s3['se'] = df_s3['se'].map('{:,.4f}'.format)
        df_s3['val'] = df_s3['val'].map('{:,.4f}'.format)
    else:
        # print(f"Unsuccessful S3 get_object response. Status - {status}")
        row = {"file_name":obj.key, "source":source_api, "skip":True, "reason": f"Unsuccessful S3 get_object response. Status - {status}"}
        dump_json(row)
        continue
    df_s3.dropna(subset=['val'], inplace=True)


    #epidata api

    #print(source_api, signal_s3, time_value_s3, geo_s3)
    response_api = Epidata.covidcast(source_api, signal_api, time_type=api_time_type,
                                  geo_type=geo_s3, time_values=time_value_s3,
                                  geo_value="*", as_of=None, lag=None)
    df_latest = pd.DataFrame.from_dict(response_api["epidata"])
    if df_latest.empty:
        df_latest = pd.DataFrame(columns=['geo_value', 'value', 'stderr', 'sample_size'])
        full_file_dif_potential = True
    df_latest = df_latest[['geo_value', 'value', 'stderr', 'sample_size']]
    df_latest.rename(columns={'geo_value': 'geo_id', 'value': 'val', 'stderr': 'se', 'sample_size': 'sample_size'}, inplace=True)
    df_latest.dropna(subset=['val'], inplace=True) #drop rows with NA values in val column
    df_latest.fillna(value=np.nan, inplace=True) #fill NA values in se and sample_size with np.nan
    if not geo_is_str:
        df_latest['geo_id'] = df_latest['geo_id'].astype(str).astype(int)
    df_latest['sample_size'] = df_latest['sample_size'].map('{:,.6f}'.format)
    df_latest['se'] = df_latest['se'].astype(float).map('{:,.4f}'.format)
    df_latest['val'] = df_latest['val'].astype(float).map('{:,.4f}'.format)

    diff = pd.concat([df_s3,df_latest]).drop_duplicates(keep=False)
    diff.dropna(subset=['val'], inplace=True)

    num_df_latest = len(df_latest.index)
    num_df_s3 = len(df_s3.index)
    number_of_dif = len(diff.index)

    if diff.empty:
        # print("No difference")
        row = {
                "file_name":obj.key,
                "source":source_api,
                "signal":signal_api,
                "time_value":time_value_s3,
                "geo_type":geo_s3,
                "dif_row_count":0,
                "s3_row_count": num_df_s3,
                "api_row_count": num_df_latest,
                "skip":False
                }
        dump_json(row)
    else:
        # print("df_s3")
        # print(df_s3)
        # print("df_latest")
        # print(df_latest)
        # print("diff")
        with open(f'diff_content_{S3_SOURCE}.txt', 'a') as f:
            f.write(f'{str(obj.key)}\n')
            f.write(f'{str(diff)}\n')
        # print(diff)
        row = {
            "file_name":obj.key,
            "source":source_api,
            "signal":signal_api,
            "time_value":time_value_s3,
            "geo_type":geo_s3,
            "dif_row_count":number_of_dif,
            "s3_row_count": num_df_s3,
            "api_row_count": num_df_latest,
            "full_dif":full_file_dif_potential,
            "skip":False,
            }
        dump_json(row)
    full_file_dif_potential = False