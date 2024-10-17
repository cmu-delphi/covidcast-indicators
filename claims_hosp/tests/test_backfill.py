import logging
import os
import glob
from datetime import datetime
from pathlib import Path

# third party
import pandas as pd
import pytest

# first party
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
TEST_LOGGER = logging.getLogger()

class TestBackfill:

    def test_store_backfill_file(self):
        dropdate = datetime(2020, 1, 1) 
        fn = "claims_hosp_as_of_20200101.parquet"
        assert fn not in os.listdir(backfill_dir)
       
        # Store backfill file
        store_backfill_file(DATA_FILEPATH, dropdate, backfill_dir)
        assert fn in os.listdir(backfill_dir)
        fn = "claims_hosp_as_of_20200101.parquet"
        backfill_df = pd.read_parquet(backfill_dir + "/"+ fn, engine='pyarrow')
        
        selected_columns = ['time_value', 'fips', 'state_id',
                        'num', 'den', 'lag', 'issue_date']
        assert set(selected_columns) == set(backfill_df.columns)  
        
        os.remove(backfill_dir + "/" + fn)
        assert fn not in os.listdir(backfill_dir)
        
    def test_merge_backfill_file(self):
        fn = "claims_hosp_202006.parquet"
        assert fn not in os.listdir(backfill_dir)
        
        # Check when there is no daily file to merge.
        today = datetime(2020, 6, 14)
        merge_backfill_file(backfill_dir, today, TEST_LOGGER,
                            test_mode=True)
        assert fn not in os.listdir(backfill_dir)
        
        # Generate backfill daily files     
        for d in range(11, 15):
            dropdate = datetime(2020, 6, d)        
            store_backfill_file(DATA_FILEPATH, dropdate, backfill_dir)
        
        # Check when the merged file is not generated
        today = datetime(2020, 7, 1)
        merge_backfill_file(backfill_dir, today, TEST_LOGGER,
                            test_mode=True)
        assert fn in os.listdir(backfill_dir)

        # Read daily file
        new_files = glob.glob(backfill_dir + "/claims_hosp_as_of_*.parquet")
        pdList = []        
        for file in new_files:
            if "from" in file:
                continue
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
        
        os.remove(backfill_dir + "/" + fn)
        assert fn not in os.listdir(backfill_dir)

    def test_merge_existing_backfill_files(self):
        issue_date = datetime(year=2020, month=6, day=13)
        issue_date_str = issue_date.strftime("%Y%m%d")
        def prep_backfill_data():
            # Generate backfill daily files
            for d in range(11, 15):
                dropdate = datetime(2020, 6, d)
                store_backfill_file(DATA_FILEPATH, dropdate, backfill_dir)

            today = datetime(2020, 6, 14)
            # creating expected file
            merge_backfill_file(backfill_dir, today, TEST_LOGGER,
                                test_mode=True)
            original = f"{backfill_dir}/claims_hosp_from_20200611_to_20200614.parquet"
            os.rename(original, f"{backfill_dir}/expected.parquet")

            # creating backfill without issue date
            os.remove(f"{backfill_dir}/claims_hosp_as_of_{issue_date_str}.parquet")
            today = datetime(2020, 6, 14)
            merge_backfill_file(backfill_dir, today,
                                test_mode=True, check_nd=2)

            old_files = glob.glob(backfill_dir + "/claims_hosp_as_of_*")
            for file in old_files:
                os.remove(file)

        prep_backfill_data()
        file_to_add = store_backfill_file(DATA_FILEPATH, issue_date, backfill_dir)
        merge_existing_backfill_files(backfill_dir, file_to_add, issue_date, TEST_LOGGER)

        expected = pd.read_parquet(f"{backfill_dir}/expected.parquet")
        merged = pd.read_parquet(f"{backfill_dir}/claims_hosp_from_20200611_to_20200614.parquet")

        check_diff = expected.merge(merged, how='left', indicator=True)
        assert check_diff[check_diff["_merge"] == "both"].shape[0] == expected.shape[0]
        for file in glob.glob(backfill_dir + "/*.parquet"):
            os.remove(file)


    def test_merge_existing_backfill_files_no_call(self):
        issue_date = datetime(year=2020, month=5, day=20)
        issue_date_str = issue_date.strftime("%Y%m%d")
        def prep_backfill_data():
            # Generate backfill daily files
            for d in range(11, 15):
                dropdate = datetime(2020, 6, d)
                store_backfill_file(DATA_FILEPATH, dropdate, backfill_dir)

            today = datetime(2020, 6, 14)
            # creating expected file
            merge_backfill_file(backfill_dir, today, TEST_LOGGER,
                                test_mode=True)

        prep_backfill_data()
        file_to_add = store_backfill_file(DATA_FILEPATH, issue_date, backfill_dir)
        merge_existing_backfill_files(backfill_dir, file_to_add, issue_date, TEST_LOGGER)

        old_files = glob.glob(backfill_dir + "*.parquet")
        for file in old_files:
            os.remove(file)



