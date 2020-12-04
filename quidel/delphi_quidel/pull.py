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

COLUMN_NAMES = {
        "covid_ag": ['SofiaSerNum', 'TestDate', 'Facility', 'City',
                     'State', 'Zip', 'PatientAge', 'Result1', 'Result2',
                     'OverallResult', 'County', 'FacilityType', 'Assay',
                     'SCO1', 'SCO2', 'CLN', 'CSN', 'InstrType',
                     'StorageDate', 'ResultId', 'SarsTestNumber'],
        "flu_ag": ['SofiaSerNum', 'TestDate', 'Facility', 'Zip',
                   'FluA', 'FluB', 'StorageDate']
}
TEST_TYPES = ["covid_ag", "flu_ag"]

def compare_dates(date1, date2, flag):
    """
    Compare two dates.

    If op == "l" return the larger date
    If op == "s" return the smaller date
    """
    if date1 > date2:
        if flag == "l":
            return date1
        return date2
    if flag == "l":
        return date2
    return date1

def check_whether_date_in_range(search_date, start_date, end_date):
    """Check whether the search date is in a valid time range."""
    if search_date > end_date:
        return False
    if search_date < start_date:
        return False
    return True

def read_historical_data():
    """Read historical flu antigen test data stored in midas /common/quidel-historical-raw."""
    pull_dir = "/common/quidel-historical-raw"
    columns = ['SofiaSerNum', 'TestDate', 'Facility', 'ZipCode',
                               'FluA', 'FluB', 'StorageDate']
    df = pd.DataFrame(columns=columns)

    for fn in os.listdir(pull_dir):
        if "xlsx" in fn:
            newdf = pd.read_excel("/".join([pull_dir, fn]))
            df = df.append(newdf[columns])
    return df

def regulate_column_names(df, test_type):
    """
    Regulate column names for flu_ag test data since Quidel changed their column names multiple times.

    We want to finalize the column name list to be:
        ['SofiaSerNum', 'TestDate', 'Facility',
        'Zip', 'FluA', 'FluB', 'StorageDate']
    """
    # No regulation needed for covid_ag test data
    if test_type == "covid_ag":
        return df

    if "AnalyteResult1" in df.keys():
        df = df.rename({"AnalyteResult1": "FluA",
                        "AnalyteResult2": "FluB"}, axis=1)
    elif "Result1" in df.keys():
        df = df.rename({"Result1": "FluA", "Result2": "FluB"}, axis=1)
    if "Zip" not in df.keys():
        df = df.rename({"ZipCode": "Zip"}, axis=1)
    return df

def get_from_email(column_names, start_dates, end_dates, mail_server,
                   account, sender, password):
    """
    Get raw data from email account.

    Parameters:
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

    Returns:
        df: pd.DataFrame
    """
    time_flag = None
    dfs = {test: pd.DataFrame(columns=column_names[test]) \
           for test in ["covid_ag", "flu_ag"]}
    start_date = compare_dates(start_dates["covid_ag"],
                               start_dates["flu_ag"], "s")
    end_date = compare_dates(end_dates["covid_ag"],
                             end_dates["flu_ag"], "l")

    with MailBox(mail_server).login(account, password, 'INBOX') as mailbox:
        for search_date in [start_date + timedelta(days=x)
                            for x in range((end_date - start_date).days + 1)]:
            for message in mailbox.fetch(A(AND(date=search_date.date(), from_=sender))):
                for att in message.attachments:
                    name = att.filename

                    # Check the test type
                    if "Sars" in name:
                        test = "covid_ag"
                    elif "Flu" in name:
                        test = "flu_ag"
                    else:
                        continue

                    # Check whether we pull the data from a valid time range
                    whether_in_range = check_whether_date_in_range(
                            search_date, start_dates[test], end_dates[test])
                    if not whether_in_range:
                        continue

                    print(f"Pulling {test} data received on %s"%search_date.date())
                    toread = io.BytesIO()
                    toread.write(att.payload)
                    toread.seek(0)  # reset the pointer
                    newdf = pd.read_excel(toread)  # now read to dataframe
                    newdf = regulate_column_names(newdf, test)
                    dfs[test] = dfs[test].append(newdf)
                    time_flag = search_date
    return dfs, time_flag

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

    Quidel antigen tests are labeled with Test Date and Storage Date. In principle,
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

def preprocess_new_data(start_dates, end_dates, mail_server, account,
                        sender, password, test_mode):
    """
    Pull and pre-process Quidel Antigen Test data from datadrop email.

    Drop unnecessary columns. Temporarily consider the positive rate
    sensor only which is related to number of total tests and number
    of positive tests.

    Parameters:
        start_dates: dict
            pull covid_ag/flu_ag data from email received from the start dates
        end_dates: dict
            pull covid_ag/flu_ag data from email received on/before the end dates
        mail_server: str
        account: str
            email account to receive new data
        sender: str
            email account of the sender
        password: str
            password of the datadrop email
        test_mode: bool
            pull raw data from email or not
    Returns:
        df: pd.DataFrame
        time_flag: datetime.date:
            the actual pull end date on which we successfully pull the data
    """
    if test_mode:
        dfs = {}
        time_flag = datetime(2020, 8, 17)
        for test_type in ["covid_ag", "flu_ag"]:
            test_data_dir = f"./test_data/{test_type}_test_data.xlsx"
            dfs[test_type] = pd.read_excel(test_data_dir)
    else:
        # Get new data from email
        dfs, time_flag = get_from_email(COLUMN_NAMES, start_dates, end_dates,
                                       mail_server, account, sender, password)

    # No new data can be pulled
    if time_flag is None:
        return dfs, time_flag

    df_finals = {}
    for test_type in TEST_TYPES:
        print(f"For {test_type}:")
        df = dfs[test_type]
        # Fix some of the fipcodes that are 9 digit instead of 5 digit
        df = fix_zipcode(df)
        # Create a column CanonicalDate according to StarageDate and TestDate
        df = fix_date(df)

        # Compute numUniqueDevices
        numUniqueDevices = df.groupby(
            by=["timestamp", "zip"],
            as_index=False)["SofiaSerNum"].agg({"SofiaSerNum": "nunique"}).rename(
                columns={"SofiaSerNum": "numUniqueDevices"}
            )

        if test_type == "covid_ag":
            # Compute overallTotal
            overall_total = df.groupby(
                by=["timestamp", "zip"],
                as_index=False)['OverallResult'].count()
            overall_total["totalTest"] = overall_total["OverallResult"]
            overall_total.drop(labels="OverallResult", axis="columns", inplace=True)

            # Compute overallPositive
            overall_pos = df[df["OverallResult"] == "positive"].groupby(
                by=["timestamp", "zip"],
                as_index=False)['OverallResult'].count()
            overall_pos["positiveTest"] = overall_pos["OverallResult"]
            overall_pos.drop(labels="OverallResult", axis="columns", inplace=True)
        else:
            # Compute overallTotal
            overall_total = df.groupby(
                by=["timestamp", "zip"],
                as_index=False)['FluA'].count()
            overall_total["totalTest"] = overall_total["FluA"]
            overall_total.drop(labels="FluA", axis="columns", inplace=True)

            # Compute overallPositive
            overall_pos = df[
                    (df["FluA"] == "positive") | (df["FluB"] == "positive")
                    ].groupby(
                by=["timestamp", "zip"],
                as_index=False)['FluA'].count()
            overall_pos["positiveTest"] = overall_pos["FluA"]
            overall_pos.drop(["FluA"], axis="columns", inplace=True)

        df_finals[test_type] = overall_total.merge(
            numUniqueDevices, on=["timestamp", "zip"], how="left"
            ).merge(
            overall_pos, on=["timestamp", "zip"], how="left"
            ).fillna(0).drop_duplicates()

    return df_finals, time_flag

def check_intermediate_file(cache_dir, pull_start_dates):
    """
    Check whether there is a cache file containing historical data already.

    Parameters:
        cache_dir: str
            where the intermediate files are stored
        pull_start_dates: dict
            keys are ["covid_ag", "flu_ag"]
            values are strings for temptorary start dates for pulling
    """
    previous_dfs = {}
    for test_type in TEST_TYPES:
        previous_dfs[test_type] = None
        if pull_start_dates[test_type] is not None:
            pull_start_dates[test_type] = datetime.strptime(
                    pull_start_dates[test_type], '%Y-%m-%d')

    for filename in os.listdir(cache_dir):
        if ".csv" in filename:
            test_type = "_".join(filename.split("_")[:2])
            date_string = filename.split("_")[4].split(".")[0]
            pull_start_dates[test_type] = datetime.strptime(date_string,
                            '%Y%m%d') + timedelta(days=1)
            previous_dfs[test_type] = pd.read_csv(join(cache_dir, filename),
                        sep=",", parse_dates=["timestamp"])
    return previous_dfs, pull_start_dates

def pull_quidel_data(params):
    """
    Pull the quidel test data and decide whether to combine the new data with stored historical records in ./cache.

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
    previous_dfs, pull_start_dates = check_intermediate_file(
        cache_dir, params["pull_start_date"].copy())

    pull_end_dates = params["pull_end_date"].copy()
    for test_type in TEST_TYPES:
        if pull_end_dates[test_type] == "":
            pull_end_dates[test_type] = datetime.today()
        else:
            pull_end_dates[test_type] = datetime.strptime(
                    pull_end_dates[test_type], '%Y-%m-%d')

    # Pull data from the email at 5 digit zipcode level
    # Use _end_date to check the most recent date that we received data
    dfs, _end_date = preprocess_new_data(
            pull_start_dates, pull_end_dates, mail_server,
            account, sender, password, test_mode)

    # Utilize previously stored data
    for test_type in TEST_TYPES:
        if previous_dfs[test_type] is not None:
            dfs[test_type] = previous_dfs[test_type].append(dfs[test_type]
                    ).groupby(["timestamp", "zip"]
                    ).sum().reset_index()
    return dfs, _end_date

def check_export_end_date(input_export_end_dates, _end_date,
                          end_from_today_minus):
    """
    Update the export_end_date according to the data received.

    By default, set the export end date to be the last pulling date - 5 days
    (END_FROM_TODAY_MINUS = 5).
    Otherwise, use the required date if it is earlier than the default one.

    Parameter:
        input_export_end_date: dict
            Read from params, values are strings of dates
        _end_date: datetime.datetime
            Updated according the data received
        END_FROM_TODAY_MINUS: int
            Report data until - X days

    Returns:
        dict: {str: datetime.datetime}
        The keys are "covid_ag" or "flu_ag"
        The values are dates from when we export data
    """
    export_end_dates = {}
    for test_type in TEST_TYPES:
        export_end_dates[test_type] = _end_date \
                                      - timedelta(days=end_from_today_minus)
        if input_export_end_dates[test_type] != "":
            input_export_end_dates[test_type] = datetime.strptime(
                    input_export_end_dates[test_type], '%Y-%m-%d')
            export_end_dates[test_type] = compare_dates(
                    input_export_end_dates[test_type],
                    export_end_dates[test_type], "s")
    return export_end_dates

def check_export_start_date(export_start_dates, export_end_dates,
                            export_day_range):
    """
    Update export_start_date according to the export_end_date so that it could be export_end_date - EXPORT_DAY_RANGE.

    Parameters:
        export_start_date: dict
            Read from params, values are strings of dates
        export_end_date: dict
            Calculated according to the data received.
            The type of values are datetime.datetime
        export_day_range: int
            Number of days to report

    Returns:
        dict: {str: datetime.datetime}
        The keys are "covid_ag" or "flu_ag"
        The values are dates until when we export data
    """
    for test_type in TEST_TYPES:
        if export_start_dates[test_type] == "":
            export_start_dates[test_type] = datetime(2020, 5, 26)
        else:
            export_start_dates[test_type] = datetime.strptime(
                    export_start_dates[test_type], '%Y-%m-%d')
        # Only export data from -45 days to -5 days
        export_start_dates[test_type] = compare_dates(
                export_end_dates[test_type] - timedelta(days=export_day_range),
                export_start_dates[test_type], "l")
        if test_type == "covid_ag":
            export_start_dates[test_type] = compare_dates(
                    export_start_dates[test_type], datetime(2020, 5, 26), "l")
    return export_start_dates

def update_cache_file(dfs, _end_date, cache_dir):
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
    for test_type in TEST_TYPES:
        for fn in os.listdir(cache_dir):
            if ".csv" in fn and test_type in fn:
                os.remove(join(cache_dir, fn))
        dfs[test_type].to_csv(join(
                cache_dir, test_type + "_pulled_until_%s.csv"
                )% _end_date.strftime("%Y%m%d"), index=False)
