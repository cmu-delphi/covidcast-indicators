import calendar
import logging
import os 
import glob
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

from delphi_utils.logger import get_structured_logger
from delphi_quidel_covidtest.backfill import (store_backfill_file,
                                              merge_backfill_file, merge_existing_backfill_files)

TEST_PATH = Path(__file__).parent
PARAMS = {
        "indicator": {
            "backfill_dir": f"{TEST_PATH}/backfill",
            "drop_date": "2020-06-11",
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
        },
    }
DATA_FILEPATH =  f"{PARAMS['indicator']['input_cache_dir']}/pulled_until_20200817.csv"
backfill_dir = PARAMS["indicator"]["backfill_dir"]

class TestBackfill:
    _end_date = datetime.strptime(DATA_FILEPATH.split("_")[2].split(".")[0],
                                        '%Y%m%d') + timedelta(days=1)
    df = pd.read_csv(DATA_FILEPATH, sep=",", parse_dates=["timestamp"])

    def cleanup(self):
        for file in glob.glob(f"{backfill_dir}/*.parquet"):
            os.remove(file)
            assert file not in os.listdir(backfill_dir)

    def test_store_backfill_file(self, caplog):
        caplog.set_level(logging.INFO)
        logger = get_structured_logger()

        store_backfill_file(self.df, datetime(2020, 1, 1), backfill_dir, logger)
        fn = "quidel_covidtest_as_of_20200101.parquet"
        assert fn in os.listdir(backfill_dir)
        assert "Stored source data in parquet" in caplog.text
        
        backfill_df = pd.read_parquet(backfill_dir + "/"+ fn, engine='pyarrow')
        selected_columns = ['time_value', 'fips', 'state_id',
                        'den_total', 'num_total',
                        'num_age_0_4', 'den_age_0_4',
                        'num_age_5_17', 'den_age_5_17',
                        'num_age_18_49', 'den_age_18_49',
                        'num_age_50_64', 'den_age_50_64',
                        'num_age_65plus', 'den_age_65plus',
                        'num_age_0_17', 'den_age_0_17',
                        'lag', 'issue_date']
        assert set(selected_columns) == set(backfill_df.columns)
        assert fn in os.listdir(backfill_dir)

        self.cleanup()
    def test_merge_backfill_file(self, caplog, monkeypatch):
        caplog.set_level(logging.INFO)
        logger = get_structured_logger()

        fn = "quidel_covidtest_202008.parquet"
        assert fn not in os.listdir(backfill_dir)
        
        # Check when no daily file stored
        today = datetime(2020, 8, 20)
        merge_backfill_file(backfill_dir, today, logger, test_mode=True)
        assert fn not in os.listdir(backfill_dir)
        assert "No new files to merge; skipping merging" in caplog.text
        
        for d in range(17, 21):
            dropdate = datetime(2020, 8, d)
            store_backfill_file(self.df, dropdate, backfill_dir, logger)

         # Generate the merged file, but not delete it
        today = datetime(2020, 9, 1)
        monkeypatch.setattr(calendar, 'monthrange', lambda x, y: (1, 4))
        merge_backfill_file(backfill_dir, today, logger, test_mode=True,)
        assert fn in os.listdir(backfill_dir)
        assert "Merging files" in caplog.text

        # Read daily file
        new_files = glob.glob(backfill_dir + "/quidel_covidtest_as_of*.parquet")
        pdList = []        
        for file in new_files:
            if "from" in file:
                continue
            df = pd.read_parquet(file, engine='pyarrow')
            pdList.append(df)
            os.remove(file)
        expected = pd.concat(pdList).sort_values(["time_value", "fips"])
        
        # Read the merged file
        merged = pd.read_parquet(backfill_dir + "/" + fn, engine='pyarrow')
        
        assert set(expected.columns) == set(merged.columns)
        assert expected.shape[0] == merged.shape[0]
        assert expected.shape[1] == merged.shape[1]
        
        self.cleanup()

    def test_merge_existing_backfill_files(self, caplog, monkeypatch):
        issue_date = datetime(year=2020, month=7, day=20)
        issue_date_str = issue_date.strftime("%Y%m%d")
        caplog.set_level(logging.INFO)
        logger = get_structured_logger()
        def prep_backfill_data():
            # Generate backfill daily files
            for d in range(18, 24):
                dropdate = datetime(2020, 7, d)
                df_part = self.df[self.df['timestamp'] == dropdate]
                store_backfill_file(df_part, dropdate, backfill_dir, logger)

            today = datetime(2020, 8, 1)
            # creating expected file
            monkeypatch.setattr(calendar, 'monthrange', lambda x, y: (1, 4))
            merge_backfill_file(backfill_dir, today, logger,
                                test_mode=True)
            original = f"{backfill_dir}/quidel_covidtest_202007.parquet"
            os.rename(original, f"{backfill_dir}/expected.parquet")

            # creating backfill without issue date
            issue_date_filename = f"{backfill_dir}/quidel_covidtest_as_of_{issue_date_str}.parquet"
            os.remove(issue_date_filename)
            merge_backfill_file(backfill_dir, today, logger,
                                test_mode=True)

            old_files = glob.glob(backfill_dir + "/quidel_covidtest_as_of_*")
            for file in old_files:
                os.remove(file)

        prep_backfill_data()

        df_to_add = self.df[self.df['timestamp'] == issue_date]
        file_to_add = store_backfill_file(df_to_add, issue_date, backfill_dir, logger)
        merge_existing_backfill_files(backfill_dir, file_to_add, issue_date, logger)

        assert "Adding missing date to merged file" in caplog.text

        expected = pd.read_parquet(f"{backfill_dir}/expected.parquet")
        merged = pd.read_parquet(f"{backfill_dir}/quidel_covidtest_202007.parquet")

        check = pd.concat([merged, expected]).drop_duplicates(keep=False)

        assert len(check) == 0
        self.cleanup()


    def test_merge_existing_backfill_files_no_call(self, caplog):
        issue_date = datetime(year=2020, month=5, day=20)
        caplog.set_level(logging.INFO)
        logger = get_structured_logger()
        def prep_backfill_data():
            # Generate backfill daily files
            for d in range(18, 24):
                dropdate = datetime(2020, 7, d)
                df_part = self.df[self.df["timestamp"] == dropdate]
                store_backfill_file(df_part, dropdate, backfill_dir, logger)

            today = datetime(2020, 8, 1)
            # creating expected file
            merge_backfill_file(backfill_dir, today, logger,
                                test_mode=True)

        prep_backfill_data()
        file_to_add = store_backfill_file(self.df, issue_date, backfill_dir, logger)
        merge_existing_backfill_files(backfill_dir, file_to_add, issue_date, logger)
        assert "Issue date has no matching merged files" in caplog.text
        self.cleanup()

        
