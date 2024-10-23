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
from pathlib import Path
from typing import Union, Dict

import boto3
import regex
from datetime import datetime
import pandas as pd
import numpy as np
import covidcast
from delphi_epidata import Epidata
from sqlalchemy import create_engine, text
import warnings
import json
import os
from dotenv import load_dotenv
load_dotenv()
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=RuntimeWarning)

# Get these from environment variables
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
API_KEY = os.getenv("API_KEY")
S3_SOURCE = os.getenv("S3_SOURCE")
SQL_HOST = os.getenv("SQL_HOST")
SQL_USER = os.getenv("SQL_USER")
SQL_PASS = os.getenv("SQL_PASS")

covidcast.use_api_key(API_KEY)
Epidata.debug = True
Epidata.auth = ("epidata", API_KEY)

client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
bucket = s3.Bucket(BUCKET_NAME)

engine = create_engine(f'mysql+pymysql://{SQL_USER}:{SQL_PASS}@{SQL_HOST}/covid').connect()

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

DROP_COLS = ["se", "sample_size",  "missing_val", "missing_se", "missing_sample_size"]

CSV_PATH = Path(__file__).parent / 'diff_csv_joined'
JSON_PATH = Path(__file__).parent / 'json_files'

def dump_json(data, source):
    f = open(f'{JSON_PATH}/{source}.json', 'a')
    json.dump(data, f)
    f.write(",\n")
    f.close()

def parse_bucket_info(obj) -> Dict:
    """
    Parse s3 csv files to get request parameters for api
    """
    prefixes = obj.key.split("/")
    source_s3 = prefixes[0]
    source_api = SOURCES[source_s3]

    # s3 file name not valid
    csvname_s3 = prefixes[1]
    if len(csvname_s3) == 0:
        row = {"file_name": obj.key, "source": source_api, "skip": True, "reason": "file has no name"}
        dump_json(row, source_api)
        return dict()

    # time_value
    if source_s3 in WEEKLY_SOURCES:
        time_value_s3 = csvname_s3.split("_")[1]
    else:
        time_value_s3 = csvname_s3.split("_")[0]

    if time_value_s3[:2] != "20":
        # print("file has non-standardized naming")
        row = {"file_name": obj.key, "source": source_api, "skip": True, "reason": "file has non-standardized naming"}
        dump_json(row, source_api)
        return dict()

    # geo
    if source_s3 in WEEKLY_SOURCES:
        geo_s3 = csvname_s3.split("_")[2]
    else:
        geo_s3 = csvname_s3.split("_")[1]

    # signal
    if source_s3 in WEEKLY_SOURCES:
        signal_s3 = regex.search(r"(?<=\w+_\d+_\w+_)\w+(?=\.csv$)", csvname_s3).group(0)
    else:
        signal_s3 = regex.search(r"(?<=\d+_\w+_)\w+(?=\.csv$)", csvname_s3).group(0)

    # remove work in progress
    if 'wip' in signal_s3:
        row = {"file_name": obj.key, "source": source_api, "skip": True, "reason": f"wip in signal name"}
        dump_json(row, source_api)
        return dict()
    else:
        signal_api = signal_s3
    return {"source_api": source_api, "signal_api": signal_api, "geo_s3": geo_s3, "signal_s3":signal_s3, "time_value_s3": time_value_s3}

def check_diff_with_merge(df_s3, df_api):
    """
    use merge to see how the difference are
    Parameters
    ----------
    df_s3
    df_api
    Returns
    -------
    """
    a, b = None, None
    suffix = None
    if len(df_latest) > len(df_s3):
        a, b = (df_api, df_s3)
        suffix = ("_api", "_s3")
    else:
        a, b = (df_s3, df_api)
        suffix = ("_s3", "_api")

    joined_diff = pd.merge(a, b, on=['geo_id'], how='left', suffixes=suffix)
    filtered_join_diff = joined_diff[joined_diff['val_api'] != joined_diff['val_s3']]
    return filtered_join_diff


if __name__ == '__main__':
    full_file_dif_potential = None
    sorted_sources = sorted(SOURCES.keys())
    for source in sorted_sources:
        if source in WEEKLY_SOURCES:
            api_time_type = "week"
        else:
            api_time_type = "day"

        obj_list = bucket.objects.filter(Prefix=source)
        obj_df = pd.DataFrame([parse_bucket_info(obj) for idx, obj in enumerate(obj_list) if len(parse_bucket_info(obj)) > 0])
        time_range = (obj_df.time_value_s3.min(), obj_df.time_value_s3.max())
        geos = obj_df.geo_s3.unique()
        signals = obj_df.signal_api.unique()

        query = f"""
            SELECT
                *
            FROM covid.epimetric_latest_v el
            WHERE 1=1
            AND el.`source` = "{SOURCES[source]}"
            AND el.`signal` in ({' '.join('"' + x + '",' for x in signals)[:-1]})
            AND el.time_type = "{api_time_type}"
            AND el.geo_type in ({' '.join('"' + x + '",' for x in geos)[:-1]})
            AND el.time_value >= {time_range[0]}
            AND el.time_value <= {time_range[1]}
        """

        df = pd.read_sql(text(query), engine)
        df = df[["source", "signal", "value", "geo_value", "geo_type", "time_value"]]

        for idx, obj in enumerate(obj_list):
            metadata = parse_bucket_info(obj)

            if len(metadata) == 0:
                continue

            source_api = metadata["source_api"]
            signal_api = metadata["signal_api"]
            geo_s3 = metadata["geo_s3"]
            signal_s3 = metadata["signal_s3"]
            time_value_s3 = int(metadata["time_value_s3"])

            response = client.get_object(Bucket=BUCKET_NAME, Key=obj.key)

            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 200:
                df_s3 = pd.read_csv(response.get("Body"))
                df_s3.dropna(subset=['val'], inplace=True)
                df_s3 = df_s3[['geo_id', 'val']]
                df_s3["val"] = df_s3["val"].astype(float)
                # round values for float precision
                df_s3 = df_s3.round({"val": 7})
            else:
                # print(f"Unsuccessful S3 get_object response. Status - {status}")
                row = {"file_name":obj.key, "source":source_api, "skip":True, "reason": f"Unsuccessful S3 get_object response. Status - {status}"}
                dump_json(row, source_api)
                continue

            df_latest = df[
                (df['source'] == source_api) &
                (df['signal'] == signal_api) &
                (df['geo_type'] == geo_s3) &
                (df['time_value'] == time_value_s3)].copy()
            
            if df_latest.empty:
                df_latest = pd.DataFrame(columns=['geo_value', 'value', 'stderr', 'sample_size'])
                full_file_dif_potential = True
            df_latest = df_latest[['geo_value', 'value']]
            df_latest.rename(columns={'geo_value': 'geo_id', 'value': 'val'}, inplace=True)
            df_latest.dropna(subset=['val'], inplace=True)
            if geo_s3 not in ["state", "nation"]:
                df_latest['geo_id'] = df_latest['geo_id'].astype(str).astype(int)
            df_latest["val"] = df_latest["val"].astype(float)
            df_latest = df_latest.round({"val": 7})

            # get difference with drop dup
            diff = pd.concat([df_s3,df_latest]).drop_duplicates(keep=False)
            diff.dropna(subset=['val'], inplace=True)

            num_df_latest = len(df_latest.index)
            num_df_s3 = len(df_s3.index)
            number_of_dif = len(diff.index)

            if diff.empty:
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
                dump_json(row, source_api)
            else:
                csv_file_split = str(obj.key).split("/")
                Path(f'{CSV_PATH}/{csv_file_split[0]}').mkdir(parents=True, exist_ok=True)
                diff = {
                    "num_rows": number_of_dif,
                }
                try:
                    diff_w_merge = check_diff_with_merge(df_s3=df_s3, df_api=df_latest)
                    diff_w_merge.to_csv(f'{CSV_PATH}/{csv_file_split[0]}/joined_{csv_file_split[1]}', index=False)
                    diff = {
                        "num_rows":number_of_dif,
                        "s3_nan_row_count": int(diff_w_merge["val_s3"].isna().sum()),
                        "api_nan_row_count": int(diff_w_merge["val_api"].isna().sum()),
                    }
                except:
                    diff.to_csv(f'{CSV_PATH}/{csv_file_split[0]}/dedup_{csv_file_split[1]}', index=False)

                row = {
                    "file_name":obj.key,
                    "source":source_api,
                    "signal":signal_api,
                    "time_value":time_value_s3,
                    "geo_type":geo_s3,
                    "s3_row_count": num_df_s3,
                    "api_row_count": num_df_latest,
                    "full_diff":full_file_dif_potential,
                    "skip":False,
                    }
                row.update(diff)
                dump_json(row, source_api)
            full_file_dif_potential = False