# -*- coding: utf-8 -*-
"""Simply downloads email attachments.
Uses this handy package: https://pypi.org/project/imap-tools/
"""
from datetime import datetime, timedelta
import io
import os

import pandas as pd
import numpy as np

from imap_tools import MailBox, A, AND

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
    df = pd.DataFrame(columns=['SofiaSerNum', 'TestDate', 'Facility', 'City',
                               'State', 'Zip', 'PatientAge', 'Result1', 'Result2',
                               'OverallResult', 'County', 'FacilityType', 'Assay',
                               'SCO1', 'SCO2', 'CLN', 'CSN', 'InstrType',
                               'StorageDate', 'ResultId', 'SarsTestNumber'])
    with MailBox(mail_server).login(account, password, 'INBOX') as mailbox:
        for search_date in [start_date + timedelta(days=x)
                            for x in range((end_date - start_date).days + 1)]:
            for message in mailbox.fetch(A(AND(date=search_date, from_=sender))):
                for att in message.attachments:
                    name = att.filename
                    # Only consider covid tests
                    if "Sars" not in name:
                        continue
                    print("Pulling data received on %s"%search_date)
                    toread = io.BytesIO()
                    toread.write(att.payload)
                    toread.seek(0)  # reset the pointer
                    newdf = pd.read_excel(toread)  # now read to dataframe
                    df = df.append(newdf)
                    time_flag = search_date
    return df, time_flag

def fix_zipcode(df):
    """
    Fix zipcode that is 9 digit instead of 5 digit
    """
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

def pull_quidel_covidtest(start_date, end_date, mail_server, account, sender, password):
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
        time_flag: datetime.date:
            the actual pull end date on which we successfully pull the data
    """
    # Get new data from email
    df, time_flag = get_from_email(start_date, end_date, mail_server, account, sender, password)

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

    df_merged = overall_total.merge(
        overall_pos,
        on=["timestamp", "zip"],
        how="outer").fillna(0).drop_duplicates()

    return df_merged, time_flag

def check_intermediate_file(cache_dir, pull_start_date):
    for filename in os.listdir(cache_dir):
        if ".csv" in filename:
            pull_start_date = datetime.strptime(filename.split("_")[2].split(".")[0],
                                            '%Y%m%d').date() + timedelta(days=1)
            return filename, pull_start_date
    return None, pull_start_date
