# -*- coding: utf-8 -*-
"""Simply downloads email attachments.
Uses this handy package: https://pypi.org/project/imap-tools/
"""
from datetime import datetime, timedelta, date
import io
import os

import pandas as pd
import numpy as np

from imap_tools import MailBox, A, AND

def read_historical_data():
    """
    read historical data stored in midas /common/quidel-historical-raw
    """
    pull_dir = "/common/quidel-historical-raw"


    columns = ['SofiaSerNum', 'TestDate', 'Facility', 'ZipCode',
                               'FluA', 'FluB', 'StorageDate']
    df = pd.DataFrame(columns=columns)

    #Read historical data (uncomment these lines until it is ready to be
    #publish. These can only run through midas)
    for fn in os.listdir(pull_dir):
        if "xlsx" in fn:
            newdf = pd.read_excel("/".join([pull_dir, fn]))
            df = df.append(newdf[columns])
    return df

def get_from_email(start_date: datetime.date, end_date: datetime.date,
                   mail_server: str, account: str, sender: str, password: str):
    """
    Get raw data from email account
    Args:
        start_date: datetime.date
            pull data from email received from the start date
        end_date: datetime.date
            pull data from email received on/before the end date
        mail_server: str
        account: str
            email account to receive new data
        sender: str
            email account of the sender
        password: str
            password of the datadrop email
    output:
        df: pd.DataFrame
    """
    time_flag = None
    columns = ['SofiaSerNum', 'TestDate', 'Facility', 'ZipCode',
                               'FluA', 'FluB', 'StorageDate']
    df = pd.DataFrame(columns=columns)

    with MailBox(mail_server).login(account, password, 'INBOX') as mailbox:
        # Read new data received from Quidel
        for search_date in [start_date + timedelta(days=x)
                            for x in range((end_date - start_date).days + 1)]:
            for message in mailbox.fetch(A(AND(date=search_date, from_=sender))):
                for att in message.attachments:
                    name = att.filename
                    # Only consider flu tests
                    if "Flu" not in name:
                        continue
                    print("Pulling data received on %s"%search_date)
                    toread = io.BytesIO()
                    toread.write(att.payload)
                    toread.seek(0)  # reset the pointer
                    newdf = pd.read_excel(toread)  # now read to dataframe
                    if "AnalyteResult1" in newdf.keys():
                        newdf = newdf.rename({"AnalyteResult1": "FluA",
                                              "AnalyteResult2": "FluB"},
                                             axis=1)
                    elif "Result1" in newdf.keys():
                        newdf = newdf.rename({"Result1": "FluA",
                                              "Result2": "FluB"},
                                             axis=1)
                    if "ZipCode" not in newdf.keys():
                        newdf = newdf.rename({"Zip": "ZipCode"}, axis=1)
                    df = df.append(newdf[columns])
                    time_flag = search_date

    return df, time_flag

def fix_zipcode(df):
    """
    Fix zipcode that is 9 digit instead of 5 digit
    """
    zipcode5 = []
    fixnum = 0
    for zipcode in df['ZipCode'].values:
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
    Quidel Covid Test are labeled with Test Date and Storage Date.
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

def pull_quidel_flutest(start_date, end_date, mail_server, account, sender, password):
    """
    Pull and pre-process Quidel Covid Test data from datadrop email.
    Drop unnecessary columns. Temporarily consider the positive rate
    sensor only which is related to number of total tests and number
    of positive tests.

    Args:
        start_date: datetime.date
            pull data from email received from the start date
        end_date: datetime.date
            pull data from email received on/before the end date
        mail_server: str
        account: str
            email account to receive new data
        sender: str
            email account of the sender
        password: str
            password of the datadrop email
    output:
        df: pd.DataFrame
    """
    # Get new data from email
    df, time_flag = get_from_email(start_date, end_date, mail_server, account, sender, password)

    # Get historical data
    if start_date < date(2020, 5, 8):
        print("Read historical data")
        df_historical = read_historical_data()
        df = df.append(df_historical)

    # No new data can be pulled
    if time_flag is None:
        return df, time_flag

    # Fix some of the fipcodes that are 9 digit instead of 5 digit
    df = fix_zipcode(df)

    # Create a column CanonicalDate according to StarageDate and TestDate
    df = fix_date(df)

    # Compute overall negative
    overall_pos = df[(df["FluA"] == "positive") | (df["FluB"] == "positive")].groupby(
        by=["timestamp", "zip"],
        as_index=False)['FluA'].count()
    overall_pos["positiveTest"] = overall_pos["FluA"]
    overall_pos.drop(["FluA"], axis="columns", inplace=True)

    # Compute overallTotal
    overall_total = df.groupby(
        by=["timestamp", "zip"],
        as_index=False)['FluA'].count()
    overall_total["totalTest"] = overall_total["FluA"]
    overall_total.drop(labels="FluA", axis="columns", inplace=True)

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
    for filename in os.listdir(cache_dir):
        if ".csv" in filename:
            pull_start_date = datetime.strptime(filename.split("_")[2].split(".")[0],
                                            '%Y%m%d').date() + timedelta(days=1)
            return filename, pull_start_date
    return None, pull_start_date
