import logging
import os 
import glob
from datetime import datetime

import pandas as pd

from delphi_quidel_covidtest.pull import (
    fix_zipcode,
    fix_date,
    pull_quidel_covidtest,
    check_intermediate_file,
    check_export_end_date,
    check_export_start_date,
    store_backfill_file,
    merge_backfill_file
)
from delphi_quidel_covidtest.constants import AGE_GROUPS

END_FROM_TODAY_MINUS = 5
EXPORT_DAY_RANGE = 40

TEST_LOGGER = logging.getLogger()
backfill_dir="./backfill"

class TestFixData:
    def test_fix_zipcode(self):

        df = pd.DataFrame({"Zip":[2837,  29570, "15213-0436", "02134-3611"]})
        df = fix_zipcode(df)

        assert set(df["zip"]) == set([2837, 29570, 15213, 2134])

    def test_fix_date(self):

        df = pd.DataFrame({"StorageDate":[datetime(2020, 5, 19), datetime(2020, 6, 9),
                                          datetime(2020, 6, 14), datetime(2020, 7, 10)],
                           "TestDate":[datetime(2020, 1, 19), datetime(2020, 6, 10),
                                          datetime(2020, 6, 11), datetime(2020, 7, 2)]})
        df = fix_date(df, TEST_LOGGER)

        assert set(df["timestamp"]) == set([datetime(2020, 5, 19),
                                            datetime(2020, 6, 11), datetime(2020, 7, 2)])

class TestingPullData:
    def test_pull_quidel_covidtest(self):

        df, _ = pull_quidel_covidtest({
            "static_file_dir": "../static",
            "input_cache_dir": "./cache",
            "export_start_date": "2020-06-30",
            "export_end_date": "",
            "pull_start_date": "2020-07-09",
            "pull_end_date":"",
            "aws_credentials": {
                "aws_access_key_id": "",
                "aws_secret_access_key": ""
            },
            "bucket_name": "",
            "wip_signal": "",
            "test_mode": True
        }, TEST_LOGGER)

        first_date = df["timestamp"].min().date()
        last_date = df["timestamp"].max().date()

        assert [first_date.month, first_date.day] == [7, 18]
        assert [last_date.month, last_date.day] == [7, 23]
        assert set(['timestamp', 'zip']).issubset(set(df.columns))
        for agegroup in AGE_GROUPS:
            set([f'totalTest_{agegroup}', f'numUniqueDevices_{agegroup}',
             f'positiveTest_{agegroup}']).issubset(set(df.columns))


    def test_check_intermediate_file(self):

        previous_df, pull_start_date = check_intermediate_file("./cache/test_cache_with_file", None)
        assert previous_df is not None
        assert pull_start_date is not None
        # Put the test file back
        previous_df.to_csv("./cache/test_cache_with_file/pulled_until_20200710.csv", index=False)

        previous_df, pull_start_date = check_intermediate_file("./cache/test_cache_without_file",
                                                               None)
        assert previous_df is None
        assert pull_start_date is None

    def test_check_export_end_date(self):

        _end_date = datetime(2020, 7, 7)
        export_end_dates = ["", "2020-07-07", "2020-06-15"]
        tested = []
        for export_end_date in export_end_dates:
            tested.append(check_export_end_date(export_end_date, _end_date,
                                                END_FROM_TODAY_MINUS))
        expected = [datetime(2020, 7, 2), datetime(2020, 7, 2), datetime(2020, 6,15)]

        assert tested == expected

    def test_check_export_start_date(self):

        export_end_date = datetime(2020, 7, 2)
        export_start_dates = ["", "2020-06-20", "2020-04-20"]
        tested = []
        for export_start_date in export_start_dates:
            tested.append(check_export_start_date(export_start_date,
                                                  export_end_date, EXPORT_DAY_RANGE))
        expected = [datetime(2020, 5, 26), datetime(2020, 6, 20), datetime(2020, 5, 26)]

        assert tested == expected
        
    def store_backfill_file(self):
        df, _end_date = pull_quidel_covidtest({
            "static_file_dir": "../static",
            "input_cache_dir": "./cache",
            "export_start_date": "2020-06-30",
            "export_end_date": "",
            "pull_start_date": "2020-07-09",
            "pull_end_date":"",
            "aws_credentials": {
                "aws_access_key_id": "",
                "aws_secret_access_key": ""
            },
            "bucket_name": "",
            "wip_signal": "",
            "test_mode": True
        }, TEST_LOGGER)
        
        store_backfill_file(df, _end_date, backfill_dir)
        fn = "quidel_covidtest_as_of_20200817.parquet"
        backfill_df = pd.read_parquet(backfill_dir + "/"+ fn, engine='pyarrow')
        
        selected_columns = ['time_value', 'fips',
                        'totalTest_total', 'positiveTest_total',
                        'positiveTest_age_0_4', 'totalTest_age_0_4',
                        'positiveTest_age_5_17', 'totalTest_age_5_17',
                        'positiveTest_age_18_49', 'totalTest_age_18_49',
                        'positiveTest_age_50_64', 'totalTest_age_50_64',
                        'positiveTest_age_65plus', 'totalTest_age_65plus',
                        'positiveTest_age_0_17', 'totalTest_age_0_17']
        assert set(selected_columns) == set(backfill_df.columns)       
        
    def test_merge_backfill_file(self):
        
        today = datetime.today()
        new_files = glob.glob(backfill_dir + "/*.parquet")
        fn = "quidel_covidtest_from_20200817_to_20200820.parquet"
        assert fn not in os.listdir(backfill_dir)
        
        # Check the when the merged file is not generated
        today = datetime(2020, 8, 20)
        merge_backfill_file(backfill_dir, today.weekday(), today, test_mode=True, check_nd=8)
        assert fn not in os.listdir(backfill_dir)
        
         # Generate the merged file, but not delete it
        merge_backfill_file(backfill_dir, today.weekday(), today, test_mode=True, check_nd=2)         
        assert fn in os.listdir(backfill_dir)

        # Read daily file
        pdList = []        
        for file in new_files:
            df = pd.read_parquet(file, engine='pyarrow')
            pdList.append(df)
        expected = pd.concat(pdList).sort_values(["time_value", "fips"])
        
        # Read the merged file
        merged = pd.read_parquet(backfill_dir + "/" + fn, engine='pyarrow')
        
        assert expected.shape[0] == merged.shape[0]
        assert expected.shape[1] == merged.shape[1]
        
        os.remove(backfill_dir + "/" + fn)
        assert fn not in os.listdir(backfill_dir)
        
