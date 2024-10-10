'''
This code is used to compare the data from JHU source with its Delphi Epidata API latest and Archive Differ S3 equivalent.
Specifically, it pulls the data from the JHU source csv files on Github.
The 2 jhu source urls contain csv files where each column is a date and each row is a location.
For each date, this script takes the data from the source csv file, puts that into a pandas df.
It then generates the euivalent dataframes from the API and S3 bucket, and put those into pandas df as well.
It then compares the 3 dataframes and output the row numbers on each side, as well as the number of rows that are different among them.

Note: The script drops rows in any of the 3 dataframes with 
- NA values 
- 0 values 
- megacounties data
before comparison.

Megacounties data are dropped because the source data does not contain them, but the API and S3 dataframes do contain them. 
This is why this script should not be used to compare API and S3 data.

There could be 4 output files to this script:
- A jhu_source_today.json that contains the number of rows in each of the 3 comparison dataframes, as well as the number of rows that are different among them.
- If there are differences between the dataframes, each kind would produce a diff_content_jhu_source_api.txt/ diff_content_jhu_source_s3.txt/ diff_content_api_s3.txt file.

Usage:
- Add AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME and API_KEY as environment variable.
- Run the script: `python jhu_source.py`
'''
import boto3
from datetime import datetime
import pandas as pd
import numpy as np
import covidcast
from delphi_epidata import Epidata
import warnings
import json
import os
import botocore
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=RuntimeWarning)

# Get these from environment variables
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
API_KEY = os.getenv("API_KEY")
S3_SOURCE = "jhu"

covidcast.use_api_key(API_KEY)
Epidata.debug = True
Epidata.auth = ("epidata", API_KEY)

client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
bucket = s3.Bucket(BUCKET_NAME)

deaths_url = f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv"
confirmed_url = f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"
sources_url = [deaths_url, confirmed_url]

signals = {
    "confirmed_cumulative_num": confirmed_url,
    "deaths_cumulative_num": deaths_url
}

def dump_json(data):
    f = open(f'jhu_source_{datetime.today().strftime('%Y%m%d')}.json', 'a')
    json.dump(data, f)
    f.write(",\n")
    f.close()

# For each signal confirmed_cumulative_num and deaths_cumulative_num
for signal in signals:
    base_df = pd.read_csv(signals[signal])
    dates = base_df.columns[12:]
    for date in dates:
        # print(date)
        source_date = datetime.strptime(date, "%m/%d/%y")
        source_date = source_date.strftime("%Y%m%d")
        row = {"date":source_date, "signal":signal}
        print(source_date, signal)

        # JHU/source
        date_df = base_df[['FIPS', 'Country_Region', date]]
        date_df = date_df[date_df['Country_Region'] == 'US']
        df_source = date_df.rename(
            columns={
                date: 'val',
                'FIPS': 'geo_id',
                })
        df_source = df_source.dropna(subset=['geo_id'])
        df_source['geo_id'] = df_source['geo_id'].astype(int).astype(str).apply(lambda x: x.zfill(5))
        df_source['val'] = df_source['val'].astype(float)
        df_source.drop(columns='Country_Region', inplace=True)
        df_source['se'] = np.nan
        df_source['sample_size'] = np.nan
        df_source.dropna(subset=['val'], inplace=True) # drop rows with NA values
        df_source = df_source.loc[df_source['val'] != '0.0000'] # drop rows with 0 values
        # print("df_source", df_source)

        #API
        response_api = Epidata.covidcast('jhu-csse', signal, time_type='day',
                                  geo_type='county', time_values=source_date,
                                  geo_value="*", as_of=None, lag=None)
        df_api = pd.DataFrame.from_dict(response_api["epidata"])
        if df_api.empty:
            df_api = pd.DataFrame(columns=['geo_value', 'value', 'stderr', 'sample_size'])
        df_api = df_api[['geo_value', 'value', 'stderr', 'sample_size']]
        df_api.rename(columns={'geo_value': 'geo_id', 'value': 'val', 'stderr': 'se', 'sample_size': 'sample_size'}, inplace=True)
        df_api.dropna(subset=['val'], inplace=True)
        df_api.fillna(value=np.nan, inplace=True)
        df_api = df_api.loc[df_api['val'] != '0.0000']
        df_api = df_api[~df_api['geo_id'].str.endswith('000')]
        # print("df_api", df_api)

        #S3
        s3_file_name = f"jhu/{source_date}_county_{signal}.csv"
        # print(s3_file_name)
        try:
            response_s3 = client.get_object(Bucket=BUCKET_NAME, Key=s3_file_name)
        except:
            row['skip_reason'] = f"File {s3_file_name} does not exist on S3"
            dump_json(row)
            continue
        status = response_s3.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            df_s3 = pd.read_csv(response_s3.get("Body"))
            df_s3['geo_id'] = df_s3['geo_id'].astype(int).astype(str).apply(lambda x: x.zfill(5))
            # print("df_s3", df_s3)
            df_s3.dropna(subset=['val'], inplace=True)
            df_s3 = df_source.loc[df_source['val'] != '0.0000']
            df_s3 = df_s3[~df_s3['geo_id'].str.endswith('000')]


        # Difference
        diff_source_api = pd.concat([df_source,df_api]).drop_duplicates(keep=False)
        diff_source_s3 = pd.concat([df_source,df_s3]).drop_duplicates(keep=False)
        diff_api_s3 = pd.concat([df_api,df_s3]).drop_duplicates(keep=False)

        # print("df_source", df_source)
        # print("df_api", df_api)
        # print(diff_source_api)

        row['source_row_count'] = len(df_source.index)
        row['api_row_count'] = len(df_api.index)
        row['s3_row_count'] = len(df_s3.index)
        row['source_api_diff_count'] = len(diff_source_api.index)
        row['source_s3_diff_count'] = len(diff_source_s3.index)
        row['api_s3_diff_count'] = len(diff_api_s3.index)
        dump_json(row)

        if row['source_api_diff_count'] !=0:
            with open(f'diff_content_jhu_source_api.txt', 'a') as f:
                f.write(f'{str(source_date)}\n')
                f.write(f'{str(diff_source_api)}\n')

        if row['source_s3_diff_count'] !=0:
            with open(f'diff_content_jhu_source_api.txt', 'a') as f:
                f.write(f'{str(source_date)}\n')
                f.write(f'{str(diff_source_s3)}\n')

        if row['api_s3_diff_count'] !=0:
            with open(f'diff_content_jhu_source_api.txt', 'a') as f:
                f.write(f'{str(source_date)}\n')
                f.write(f'{str(diff_api_s3)}\n')
        continue