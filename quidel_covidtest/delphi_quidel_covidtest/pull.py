# -*- coding: utf-8 -*-
"""Collect and process Quidel export files."""
from os.path import join
import os
from datetime import datetime, timedelta
import boto3

import pandas as pd
import numpy as np

def get_from_s3(start_date, end_date, bucket):
    """
    Get raw data from aws s3 bucket.

    Args:
        start_date: datetime.datetime
            pull data from file tagged with date on/after the start date
        end_date: datetime.datetime
            pull data from file tagged with date on/before the end date
        bucket: s3.Bucket
            the aws s3 bucket that stores quidel data
    output:
        df: pd.DataFrame
        time_flag: datetime.datetime
    """
    time_flag = None
    selected_columns = ['SofiaSerNum', 'TestDate', 'Facility', 'City',
                               'State', 'Zip', 'PatientAge', 'Result1',
                               'Result2', 'OverallResult', 'StorageDate',
                               'fname']
    df = pd.DataFrame(columns=selected_columns)
    s3_files = {}
    for obj in bucket.objects.all():
        if "-sars" in obj.key:
            date_string = obj.key.split("/")[1]
            yy = int(date_string.split("_")[0])
            mm = int(date_string.split("_")[1])
            dd = int(date_string.split("_")[2])
            received_date = datetime(yy, mm, dd)
            s3_files[received_date] = obj.key

    n_days = (end_date - start_date).days + 1
    for search_date in [start_date + timedelta(days=x) for x in range(n_days)]:
        if search_date in s3_files.keys():
            # Avoid appending duplicate datasets
            if s3_files[search_date] in set(df["fname"].values):
                continue
            print("Pulling data received on %s"%search_date.date())
            obj = bucket.Object(key=s3_files[search_date])
            newdf = pd.read_csv(obj.get()["Body"],
                                parse_dates=["StorageDate", "TestDate"],
                                low_memory=False)
            newdf["fname"] = s3_files[search_date]
            df = df.append(newdf[selected_columns])
            assert set(df.columns) == set(selected_columns)
            time_flag = search_date
    return df, time_flag

def fix_zipcode(df):
    """Fix zipcode that is 9 digit instead of 5 digit."""
    zipcode5 = []
    fixnum = 0
    for zipcode in df['Zip'].values:
        if isinstance(zipcode, str) and '-' in zipcode:
            zipcode5.append(int(zipcode.split('-')[0]))
            fixnum += 1
        else:
            zipcode = int(float(zipcode))
            zipcode5.append(zipcode)
    df['zip'] = zipcode5
    # print('Fixing %.2f %% of the data' % (fixnum * 100 / len(zipcode5)))
    return df

def fix_date(df):
    """
    Remove invalid dates and select correct test date to use.

    Quidel Covid Test are labeled with Test Date and Storage Date. In principle,
    the TestDate should reflect when the test was performed and the StorageDate
    when the test was logged in the MyVirena cloud storage device. We expect
    that the test date should precede the storage date by several days. However,
    in the actual data the test date can be far earlier than the storage date
    and the test date can also occur after the storage date.

    - For most of the cases, use test date as the timestamp
    - Remove tests with a storage date which is earlier than the test date
    - If the storage date is 90 days later than the test date, the storage
      will be adopted instead
    """
    df.insert(2, "timestamp", df["TestDate"])

    mask = df["TestDate"] <= df["StorageDate"]
    print("Removing %.2f%% of unusual data" % ((len(df) - np.sum(mask)) * 100 / len(df)))
    df = df[mask]

    mask = df["StorageDate"] - df["TestDate"] > pd.Timedelta(days=90)
    print("Fixing %.2f%% of outdated data" % (np.sum(mask) * 100 / len(df)))
    df["timestamp"].values[mask] = df["StorageDate"].values[mask]
    return df

def preprocess_new_data(start_date, end_date, params, test_mode):
    """
    Pull and pre-process Quidel Covid Test data.

    Drop unnecessary columns. Temporarily consider the positive rate
    sensor only which is related to number of total tests and number
    of positive tests.

    Args:
        start_date: datetime.datetime
            pull data from file tagged with date on/after start date
        end_date: datetime.datetime
            pull data from file tagged with date on/before the end date
        params: dict
            read from params.json
        test_mode: bool
            pull raw data from s3 or not
    output:
        df: pd.DataFrame
        time_flag: datetime.date:
            the actual pull end date on which we successfully pull the data
    """
    if test_mode:
        test_data_dir = "./test_data/test_data.csv"
        df, time_flag = pd.read_csv(
            test_data_dir,
            parse_dates=["StorageDate", "TestDate"]
            ), datetime(2020, 8, 17)
    else:
        # connect aws s3 bucket
        aws_access_key_id = params["aws_credentials"]["aws_access_key_id"]
        aws_secret_access_key = params["aws_credentials"]["aws_secret_access_key"]
        bucket_name = params["bucket_name"]

        s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)
        bucket = s3.Bucket(bucket_name)
        # Get new data from s3
        df, time_flag = get_from_s3(start_date, end_date, bucket)

    # No new data can be pulled
    if time_flag is None:
        return df, time_flag

    # Fix some of the fipcodes that are 9 digit instead of 5 digit
    df = fix_zipcode(df)

    # Create a column CanonicalDate according to StarageDate and TestDate
    df = fix_date(df)

    # Compute overallPositive
    overall_pos = df[df["OverallResult"] == "positive"].groupby(
        by=["timestamp", "zip"],
        as_index=False)['OverallResult'].count()
    overall_pos["positiveTest"] = overall_pos["OverallResult"]
    overall_pos.drop(labels="OverallResult", axis="columns", inplace=True)

    # Compute overallTotal
    overall_total = df.groupby(
        by=["timestamp", "zip"],
        as_index=False)['OverallResult'].count()
    overall_total["totalTest"] = overall_total["OverallResult"]
    overall_total.drop(labels="OverallResult", axis="columns", inplace=True)

    # Compute numUniqueDevices
    numUniqueDevices = df.groupby(
        by=["timestamp", "zip"],
        as_index=False)["SofiaSerNum"].agg({"SofiaSerNum": "nunique"}).rename(
            columns={"SofiaSerNum": "numUniqueDevices"}
        )

    df_merged = overall_total.merge(
        numUniqueDevices, on=["timestamp", "zip"], how="left"
        ).merge(
        overall_pos, on=["timestamp", "zip"], how="left"
        ).fillna(0).drop_duplicates()


    return df_merged, time_flag

def check_intermediate_file(cache_dir, pull_start_date):
    """Check whether there is a cache file containing historical data already."""
    for filename in os.listdir(cache_dir):
        if ".csv" in filename:
            pull_start_date = datetime.strptime(filename.split("_")[2].split(".")[0],
                                            '%Y%m%d') + timedelta(days=1)
            previous_df = pd.read_csv(os.path.join(cache_dir, filename),
                                      sep=",", parse_dates=["timestamp"])
            return previous_df, pull_start_date
    return None, pull_start_date

def pull_quidel_covidtest(params):
    """Pull the quidel covid test data.

    Conditionally merge new data with historical data from ./cache.

    Parameters:
        params: dict
            including all the information read from params.json
        end_from_today_minus: int
            report data until - X days
        export_day_range: int
            number of dates to report

    Returns:
        DataFrame:
            A data frame containinig the pre-process data with columns:
            timestamp, numUniqueDevices, positiveTest, totalTest
        datetime.datetime
            the first date of the report
        datetime.datetime
            the last date of the report

    """
    cache_dir = params["cache_dir"]

    test_mode = (params["mode"] == "test")

    # pull new data only that has not been ingested
    previous_df, pull_start_date = check_intermediate_file(
        cache_dir,
        datetime.strptime(params["pull_start_date"], '%Y-%m-%d'))

    if params["pull_end_date"] == "":
        pull_end_date = datetime.today()
    else:
        pull_end_date = datetime.strptime(params["pull_end_date"], '%Y-%m-%d')

    # Pull data from the file at 5 digit zipcode level
    # Use _end_date to check the most recent date that we received data
    df, _end_date = preprocess_new_data(
            pull_start_date, pull_end_date, params, test_mode)

    # Utilize previously stored data
    if previous_df is not None:
        df = previous_df.append(df).groupby(["timestamp", "zip"]).sum().reset_index()
    return df, _end_date

def check_export_end_date(input_export_end_date, _end_date,
                          end_from_today_minus):
    """
    Update the export_end_date according to the data received.

    By default, set the export end date to be the last pulling date - 5 days
    (end_from_today_minus = 5).
    Otherwise, use the required date if it is earlier than the default one.

    Parameter:
        input_export_end_date: str
            read from params
        _end_date: datetime.datetime
            updated according the data received
        end_from_today_minus: int
            report data until - X days

    Returns:
        datetime.datetime
            export data from which date
    """
    export_end_date = _end_date - timedelta(days=end_from_today_minus)
    if input_export_end_date != "":
        input_export_end_date = datetime.strptime(input_export_end_date, '%Y-%m-%d')
        if input_export_end_date < export_end_date:
            return input_export_end_date
    return export_end_date

def check_export_start_date(export_start_date, export_end_date,
                            export_day_range):
    """
    Ensure that the starte date, end date, and day range are mutually consistent.

    Parameters:
        export_start_date: str
            Read from params
        export_end_date: datetime.datetime
            Calculated according to the data received
        export_day_range: int
            Number of days to report

    Returns:
        datetime.datetime
            export data until which date

    """
    if export_start_date == "":
        export_start_date = datetime(2020, 5, 26)
    else:
        export_start_date = datetime.strptime(export_start_date, '%Y-%m-%d')
     # Only export data from -45 days to -5 days
    if (export_end_date - export_start_date).days > export_day_range:
        export_start_date = export_end_date - timedelta(days=export_day_range)

    if export_start_date < datetime(2020, 5, 26):
        return datetime(2020, 5, 26)
    return export_start_date

def update_cache_file(df, _end_date, cache_dir):
    """
    Update cache file. Remove the old one, export the new one.

    Parameter:
        df: pd.DataFrame
            Pre-process file at ZipCode level
        _end_date:
            The most recent date when the raw data is received
        cache_dir:
            ./cache where the cache file is stored
    """
    for fn in os.listdir(cache_dir):
        if ".csv" in fn:
            os.remove(join(cache_dir, fn))
    df.to_csv(join(cache_dir, "pulled_until_%s.csv") % _end_date.strftime("%Y%m%d"), index=False)
