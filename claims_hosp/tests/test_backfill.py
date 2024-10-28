import calendar
import logging
import os
import glob
from datetime import datetime
from pathlib import Path

# third party
import pandas as pd
import pytest

# first party
from delphi_utils.logger import get_structured_logger
from delphi_claims_hosp.config import Config, GeoConstants
from delphi_claims_hosp.backfill import store_backfill_file, merge_backfill_file, merge_existing_backfill_files

CONFIG = Config()
CONSTANTS = GeoConstants()
TEST_PATH = Path(__file__).parent
PARAMS = {
    "indicator": {
        "input_file": f"{TEST_PATH}/test_data/SYNEDI_AGG_INPATIENT_11062020_1451CDT.csv.gz",
        "backfill_dir": f"{TEST_PATH}/backfill",
        "drop_date": "2020-06-11",
    }
}
DATA_FILEPATH = PARAMS["indicator"]["input_file"]
DROP_DATE = pd.to_datetime(PARAMS["indicator"]["drop_date"])
backfill_dir = PARAMS["indicator"]["backfill_dir"]

class TestBackfill:

    def cleanup(self):
        for file in glob.glob(f"{backfill_dir}/*.parquet"):
            os.remove(file)

    def test_store_backfill_file(self, caplog):
        dropdate = datetime(2020, 1, 1)
        fn = "claims_hosp_as_of_20200101.parquet"
        caplog.set_level(logging.INFO)
        logger = get_structured_logger()
        num_rows = len(pd.read_csv(DATA_FILEPATH))

        # Store backfill file
        store_backfill_file(DATA_FILEPATH, dropdate, backfill_dir, logger)
        assert fn in os.listdir(backfill_dir)
        assert "Stored backfill data in parquet" in caplog.text


        fn = "claims_hosp_as_of_20200101.parquet"
        backfill_df = pd.read_parquet(backfill_dir + "/"+ fn, engine='pyarrow')

        selected_columns = ['time_value', 'fips', 'state_id',
                        'num', 'den', 'lag', 'issue_date']

        assert set(selected_columns) == set(backfill_df.columns)
        assert num_rows == len(backfill_df)

        self.cleanup()
        
    def test_merge_backfill_file(self, caplog, monkeypatch):
        fn = "claims_hosp_202006.parquet"
        caplog.set_level(logging.INFO)
        logger = get_structured_logger()

        # Check when there is no daily file to merge.
        today = datetime(2020, 6, 14)
        merge_backfill_file(backfill_dir, today, logger,
                            test_mode=True)
        assert fn not in os.listdir(backfill_dir)
        assert "No new files to merge; skipping merging" in caplog.text


        # Generate backfill daily files
        for d in range(11, 15):
            dropdate = datetime(2020, 6, d)
            store_backfill_file(DATA_FILEPATH, dropdate, backfill_dir, logger)

        today = datetime(2020, 7, 1)
        monkeypatch.setattr(calendar, 'monthrange', lambda x, y: (1, 4))
        merge_backfill_file(backfill_dir, today, logger,
                            test_mode=True)
        assert "Merging files" in caplog.text
        assert fn in os.listdir(backfill_dir)

        # Read daily file
        new_files = glob.glob(backfill_dir + "/claims_hosp_as_of_*.parquet")
        pdList = []
        for file in new_files:
            df = pd.read_parquet(file, engine='pyarrow')
            pdList.append(df)
            os.remove(file)
        new_files = glob.glob(backfill_dir + "/claims_hosp*.parquet")
        assert len(new_files) == 1

        expected = pd.concat(pdList).sort_values(["time_value", "fips"])

        # Read the merged file
        merged = pd.read_parquet(backfill_dir + "/" + fn, engine='pyarrow')

        assert set(expected.columns) == set(merged.columns)
        assert expected.shape[0] == merged.shape[0]
        assert expected.shape[1] == merged.shape[1]

        self.cleanup()

    def test_merge_backfill_file_no_call(self, caplog):
        fn = "claims_hosp_202006.parquet"
        caplog.set_level(logging.INFO)
        logger = get_structured_logger()

        # Check when there is no daily file to merge.
        today = datetime(2020, 6, 14)
        merge_backfill_file(backfill_dir, today, logger,
                            test_mode=True)
        assert fn not in os.listdir(backfill_dir)
        assert "No new files to merge; skipping merging" in caplog.text

        # Generate backfill daily files
        for d in range(11, 15):
            dropdate = datetime(2020, 6, d)
            store_backfill_file(DATA_FILEPATH, dropdate, backfill_dir, logger)

        today = datetime(2020, 7, 1)
        merge_backfill_file(backfill_dir, today, logger,
                            test_mode=True)
        assert "Not enough days, skipping merging" in caplog.text
        self.cleanup()

    def test_merge_existing_backfill_files(self, caplog):
        issue_date = datetime(year=2020, month=6, day=13)
        issue_date_str = issue_date.strftime("%Y%m%d")
        caplog.set_level(logging.INFO)
        logger = get_structured_logger()
        def prep_backfill_data():
            # Generate backfill daily files
            for d in range(11, 15):
                dropdate = datetime(2020, 6, d)
                store_backfill_file(DATA_FILEPATH, dropdate, backfill_dir, logger)

            today = datetime(2020, 7, 1)
            # creating expected file
            merge_backfill_file(backfill_dir, today, logger,
                                test_mode=True)
            original = f"{backfill_dir}/claims_hosp_202006.parquet"
            os.rename(original, f"{backfill_dir}/expected.parquet")

            # creating backfill without issue date
            os.remove(f"{backfill_dir}/claims_hosp_as_of_{issue_date_str}.parquet")
            merge_backfill_file(backfill_dir, today, logger,
                                test_mode=True)

            old_files = glob.glob(backfill_dir + "/claims_hosp_as_of_*")
            for file in old_files:
                os.remove(file)

        prep_backfill_data()
        file_to_add = store_backfill_file(DATA_FILEPATH, issue_date, backfill_dir, logger)
        merge_existing_backfill_files(backfill_dir, file_to_add, issue_date, logger)

        assert "Adding missing date to merged file" in caplog.text

        expected = pd.read_parquet(f"{backfill_dir}/expected.parquet")
        merged = pd.read_parquet(f"{backfill_dir}/claims_hosp_202006.parquet")

        check = pd.concat([merged, expected]).drop_duplicates(keep=False)

        assert len(check) == 0

        self.cleanup()


    def test_merge_existing_backfill_files_no_call(self, caplog):
        issue_date = datetime(year=2020, month=5, day=20)
        caplog.set_level(logging.INFO)
        logger = get_structured_logger()
        def prep_backfill_data():
            # Generate backfill daily files
            for d in range(11, 15):
                dropdate = datetime(2020, 6, d)
                store_backfill_file(DATA_FILEPATH, dropdate, backfill_dir, logger)

            today = datetime(2020, 6, 14)
            # creating expected file
            merge_backfill_file(backfill_dir, today, logger,
                                test_mode=True)

        prep_backfill_data()
        file_to_add = store_backfill_file(DATA_FILEPATH, issue_date, backfill_dir, logger)
        merge_existing_backfill_files(backfill_dir, file_to_add, issue_date, logger)
        assert "Issue date has no matching merged files" in caplog.text
        self.cleanup()




