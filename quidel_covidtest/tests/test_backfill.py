import logging
import os 
import glob
from datetime import datetime

import pandas as pd

from delphi_quidel_covidtest.pull import pull_quidel_covidtest

from delphi_quidel_covidtest.backfill import (store_backfill_file,
                                              merge_backfill_file)

END_FROM_TODAY_MINUS = 5
EXPORT_DAY_RANGE = 40

TEST_LOGGER = logging.getLogger()
backfill_dir="./backfill"

class TestBackfill:
        
    def test_store_backfill_file(self):
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
        
        store_backfill_file(df, datetime(2020, 1, 1), backfill_dir)
        fn = "quidel_covidtest_as_of_20200101.parquet"
        assert fn in os.listdir(backfill_dir)
        
        backfill_df = pd.read_parquet(backfill_dir + "/"+ fn, engine='pyarrow')
        
        selected_columns = ['time_value', 'fips',
                        'den_total', 'num_total',
                        'num_age_0_4', 'den_age_0_4',
                        'num_age_5_17', 'den_age_5_17',
                        'num_age_18_49', 'den_age_18_49',
                        'num_age_50_64', 'den_age_50_64',
                        'num_age_65plus', 'den_age_65plus',
                        'num_age_0_17', 'den_age_0_17']
        assert set(selected_columns) == set(backfill_df.columns)  
        
        os.remove(backfill_dir + "/" + fn)
        assert fn not in os.listdir(backfill_dir)
        
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
            issue_date = datetime.strptime(file[-16:-8], "%Y%m%d")
            df["issue_date"] = issue_date
            df["lag"] = [(issue_date - x).days for x in df["time_value"]]
            pdList.append(df)
        expected = pd.concat(pdList).sort_values(["time_value", "fips"])
        
        # Read the merged file
        merged = pd.read_parquet(backfill_dir + "/" + fn, engine='pyarrow')
        
        assert set(expected.columns) == set(merged.columns)
        assert expected.shape[0] == merged.shape[0]
        assert expected.shape[1] == merged.shape[1]
        
        os.remove(backfill_dir + "/" + fn)
        assert fn not in os.listdir(backfill_dir)
        
