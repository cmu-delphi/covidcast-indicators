# -*- coding: utf-8 -*-
"""Simply downloads email attachments.
Uses this handy package: https://pypi.org/project/imap-tools/
"""
import io
from os.path import join
import os
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

from imap_tools import MailBox, A, AND

def get_from_email(start_date, end_date, mail_server,
                   account, sender, password):
    """
    Get raw data from email account
    Args:
        start_date: datetime.datetime
            pull data from email received from the start date
        end_date: datetime.datetime
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
            for message in mailbox.fetch(A(AND(date=search_date.date(), from_=sender))):
                for att in message.attachments:
                    name = att.filename
                    # Only consider covid tests
                    if "Sars" not in name:
                        continue
                    print("Pulling data received on %s"%search_date.date())
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

def preprocess_new_data(start_date, end_date, mail_server, account,
                        sender, password, test_mode):
    """
    Pull and pre-process Quidel Covid Test data from datadrop email.
    Drop unnecessary columns. Temporarily consider the positive rate
    sensor only which is related to number of total tests and number
    of positive tests.

    Args:
        start_date: datetime.datetime
            pull data from email received from the start date
        end_date: datetime.datetime
            pull data from email received on/before the end date
        mail_server: str
        account: str
            email account to receive new data
        sender: str
            email account of the sender
        password: str
            password of the datadrop email
        test_mode: bool
            pull raw data from email or not
    output:
        df: pd.DataFrame
        time_flag: datetime.date:
            the actual pull end date on which we successfully pull the data
    """
    if test_mode:
        test_data_dir = "./test_data/test_data.xlsx"
        df, time_flag = pd.read_excel(test_data_dir), datetime(2020, 8, 17)
    else:
        # Get new data from email
        df, time_flag = get_from_email(start_date, end_date, mail_server,
                                       account, sender, password)

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
    """
    Check whether there is a cache file containing historical data already
    """
    for filename in os.listdir(cache_dir):
        if ".csv" in filename:
            pull_start_date = datetime.strptime(filename.split("_")[2].split(".")[0],
                                            '%Y%m%d') + timedelta(days=1)
            previous_df = pd.read_csv(os.path.join(cache_dir, filename),
                                      sep=",", parse_dates=["timestamp"])
            return previous_df, pull_start_date
    return None, pull_start_date

def pull_quidel_covidtest(params):
    """
    Pull the quidel covid test data. Decide whether to combine the newly
    received data with stored historical records in ./cache

    Parameters:
        params: dict
            including all the information read from params.json
        END_FROM_TODAY_MINUS: int
            report data until - X days
        EXPORT_DAY_RANGE: int
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

    mail_server = params["mail_server"]
    account = params["account"]
    password = params["password"]
    sender = params["sender"]

    test_mode = (params["mode"] == "test")

    # pull new data only that has not been ingested
    previous_df, pull_start_date = check_intermediate_file(
        cache_dir,
        datetime.strptime(params["pull_start_date"], '%Y-%m-%d'))

    if params["pull_end_date"] == "":
        pull_end_date = datetime.today()
    else:
        pull_end_date = datetime.strptime(params["pull_end_date"], '%Y-%m-%d')

    # Pull data from the email at 5 digit zipcode level
    # Use _end_date to check the most recent date that we received data
    df, _end_date = preprocess_new_data(
            pull_start_date, pull_end_date, mail_server,
            account, sender, password, test_mode)

    # Utilize previously stored data
    if previous_df is not None:
        df = previous_df.append(df).groupby(["timestamp", "zip"]).sum().reset_index()
    return df, _end_date

def check_export_end_date(input_export_end_date, _end_date,
                          END_FROM_TODAY_MINUS):
    """
    Update the export_end_date according to the data received
    By default, set the export end date to be the last pulling date - 5 days
    (END_FROM_TODAY_MINUS = 5).
    Otherwise, use the required date if it is earlier than the default one.

    Parameter:
        input_export_end_date: str
            read from params
        _end_date: datetime.datetime
            updated according the data received
        END_FROM_TODAY_MINUS: int
            report data until - X days

    Returns:
        datetime.datetime
            export data from which date
    """
    export_end_date = _end_date - timedelta(days=END_FROM_TODAY_MINUS)
    if input_export_end_date != "":
        input_export_end_date = datetime.strptime(input_export_end_date, '%Y-%m-%d')
        if input_export_end_date < export_end_date:
            return input_export_end_date
    return export_end_date

def check_export_start_date(export_start_date, export_end_date,
                            EXPORT_DAY_RANGE):
    """
    Update the export_start_date according to the export_end_date so that it
    could be export_end_date - EXPORT_DAY_RANGE

    Parameters:
        export_start_date: str
            Read from params
        export_end_date: datetime.datetime
            Calculated according to the data received
        EXPORT_DAY_RANGE: int
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
    if (export_end_date - export_start_date).days > EXPORT_DAY_RANGE:
        export_start_date = export_end_date - timedelta(days=EXPORT_DAY_RANGE)

    if export_start_date < datetime(2020, 5, 26):
        return datetime(2020, 5, 26)
    return export_start_date

def update_cache_file(df, _end_date, cache_dir):
    """
    Update cache file. Remove the old one, export the new one

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
