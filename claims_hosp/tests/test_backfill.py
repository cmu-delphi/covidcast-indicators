import os
import glob
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

# third party
import pandas as pd
import pytest

# first party
from delphi_claims_hosp.config import Config, GeoConstants
from delphi_claims_hosp.backfill import (store_backfill_file, merge_backfill_file,
                                         get_merged_file_for_date, merge_existing_backfill_files)

TEST_LOGGER = Mock()
CONFIG = Config()
CONSTANTS = GeoConstants()
PARAMS = {
    "indicator": {
        "input_file": "test_data/SYNEDI_AGG_INPATIENT_11062020_1451CDT.csv.gz",
        "backfill_dir": "./backfill",
        "drop_date": "2020-06-11",
    }
}
DATA_FILEPATH = PARAMS["indicator"]["input_file"]
DROP_DATE = pd.to_datetime(PARAMS["indicator"]["drop_date"])
backfill_dir = PARAMS["indicator"]["backfill_dir"]

class TestBackfill:

    def test_store_backfill_file(self):
        dropdate = datetime(2020, 1, 1) 
        fn = "claims_hosp_as_of_20200101.parquet"
        assert fn not in os.listdir(backfill_dir)
       
        # Store backfill file
        store_backfill_file(DATA_FILEPATH, dropdate, backfill_dir, TEST_LOGGER)
        assert fn in os.listdir(backfill_dir)
        fn = "claims_hosp_as_of_20200101.parquet"
        backfill_df = pd.read_parquet(backfill_dir + "/"+ fn, engine='pyarrow')
        
        selected_columns = ['time_value', 'fips', 'state_id',
                            'num', 'den', 'lag', 'issue_date', 'num_flu']
        assert set(selected_columns) == set(backfill_df.columns)  
        
        os.remove(backfill_dir + "/" + fn)
        assert fn not in os.listdir(backfill_dir)
        
    def test_merge_backfill_file(self):
        
        today = datetime.today()
        
        fn = "claims_hosp_from_20200611_to_20200614.parquet"
        assert fn not in os.listdir(backfill_dir)
        
        # Check when there is no daily file to merge.
        today = datetime(2020, 6, 14)
        merge_backfill_file(backfill_dir, today.weekday(), today, TEST_LOGGER,
                            test_mode=True, check_nd=8)
        assert fn not in os.listdir(backfill_dir)
        
        # Generate backfill daily files     
        for d in range(11, 15):
            dropdate = datetime(2020, 6, d)        
            store_backfill_file(DATA_FILEPATH, dropdate, backfill_dir, TEST_LOGGER)
        
        # Check the when the merged file is not generated
        today = datetime(2020, 6, 14)
        merge_backfill_file(backfill_dir, today.weekday(), today, TEST_LOGGER,
                            test_mode=True, check_nd=8)
        assert fn not in os.listdir(backfill_dir)
        
         # Generate the merged file, but not delete it
        merge_backfill_file(backfill_dir, today.weekday(), today, TEST_LOGGER,
                            test_mode=True, check_nd=2)         
        assert fn in os.listdir(backfill_dir)

        # Read daily file
        new_files = glob.glob(backfill_dir + "/claims_hosp*.parquet")
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


class TestMergeExistingBackfillFiles:

    def cleanup(self):
        for fn in self.parquet_files():
            os.remove(backfill_dir + "/" + fn)

    def parquet_files(self):
        return sorted(os.path.basename(fn)
                      for fn in glob.glob(backfill_dir + "/claims_hosp*.parquet"))

    def make_merged_file(self, start_date, end_date):
        """Write a merged file spanning start_date to end_date, one issue per day."""
        dfs = []
        for d in range((end_date - start_date).days + 1):
            dropdate = start_date + timedelta(days=d)
            fn = store_backfill_file(DATA_FILEPATH, dropdate, backfill_dir, TEST_LOGGER)
            dfs.append(pd.read_parquet(fn, engine="pyarrow"))
            os.remove(fn)
        merged = pd.concat(dfs).sort_values(["time_value", "fips"])
        fn = "claims_hosp_from_%s_to_%s.parquet" % (
            start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d"))
        merged.to_parquet(backfill_dir + "/" + fn, index=False)
        return fn

    def test_get_merged_file_for_date(self):
        self.cleanup()
        fn = self.make_merged_file(datetime(2020, 6, 11), datetime(2020, 6, 14))

        # inside the span: same file, same name
        old, new = get_merged_file_for_date(backfill_dir, datetime(2020, 6, 12))
        assert old.name == fn and new.name == fn

        # boundaries of the span count as inside
        old, new = get_merged_file_for_date(backfill_dir, datetime(2020, 6, 11))
        assert old.name == fn and new.name == fn

        # one day after the span: file gets renamed to cover it
        old, new = get_merged_file_for_date(backfill_dir, datetime(2020, 6, 15))
        assert old.name == fn
        assert new.name == "claims_hosp_from_20200611_to_20200615.parquet"

        # one day before the span: same, in the other direction
        old, new = get_merged_file_for_date(backfill_dir, datetime(2020, 6, 10))
        assert old.name == fn
        assert new.name == "claims_hosp_from_20200610_to_20200614.parquet"

        # nowhere near the span: leave it to the weekly merge
        assert get_merged_file_for_date(backfill_dir, datetime(2020, 7, 1)) == (None, None)

        self.cleanup()

    def test_merge_existing_backfill_files(self):
        self.cleanup()
        # a merged file with 06-13 missing, as if the indicator failed that day
        dfs = []
        for d in [11, 12, 14]:
            dropdate = datetime(2020, 6, d)
            fn = store_backfill_file(DATA_FILEPATH, dropdate, backfill_dir, TEST_LOGGER)
            dfs.append(pd.read_parquet(fn, engine="pyarrow"))
            os.remove(fn)
        merged_fn = "claims_hosp_from_20200611_to_20200614.parquet"
        pd.concat(dfs).sort_values(["time_value", "fips"]).to_parquet(
            backfill_dir + "/" + merged_fn, index=False)

        issue_date = datetime(2020, 6, 13)
        backfill_file = store_backfill_file(DATA_FILEPATH, issue_date, backfill_dir, TEST_LOGGER)
        merge_existing_backfill_files(backfill_dir, backfill_file, issue_date, TEST_LOGGER)

        # the daily file is folded in and cleaned up, the name is unchanged
        assert not os.path.exists(backfill_file)
        assert self.parquet_files() == [merged_fn]
        merged = pd.read_parquet(backfill_dir + "/" + merged_fn, engine="pyarrow")
        assert set(merged["issue_date"]) == {"2020-06-11", "2020-06-12", "2020-06-13", "2020-06-14"}
        assert merged.equals(merged.sort_values(["time_value", "fips"]))
        n_rows = len(merged)

        # re-running the same patch replaces the rows instead of doubling them
        backfill_file = store_backfill_file(DATA_FILEPATH, issue_date, backfill_dir, TEST_LOGGER)
        merge_existing_backfill_files(backfill_dir, backfill_file, issue_date, TEST_LOGGER)
        merged = pd.read_parquet(backfill_dir + "/" + merged_fn, engine="pyarrow")
        assert len(merged) == n_rows

        self.cleanup()

    def test_merge_existing_backfill_files_extends_span(self):
        self.cleanup()
        self.make_merged_file(datetime(2020, 6, 11), datetime(2020, 6, 14))

        issue_date = datetime(2020, 6, 15)
        backfill_file = store_backfill_file(DATA_FILEPATH, issue_date, backfill_dir, TEST_LOGGER)
        merge_existing_backfill_files(backfill_dir, backfill_file, issue_date, TEST_LOGGER)

        assert self.parquet_files() == ["claims_hosp_from_20200611_to_20200615.parquet"]
        self.cleanup()

    def test_merge_existing_backfill_files_no_match(self):
        self.cleanup()
        merged_fn = self.make_merged_file(datetime(2020, 6, 11), datetime(2020, 6, 14))

        issue_date = datetime(2020, 7, 1)
        backfill_file = store_backfill_file(DATA_FILEPATH, issue_date, backfill_dir, TEST_LOGGER)
        merge_existing_backfill_files(backfill_dir, backfill_file, issue_date, TEST_LOGGER)

        # nothing covers this date, so the daily file is left for the weekly merge
        assert os.path.exists(backfill_file)
        assert self.parquet_files() == sorted(
            [merged_fn, "claims_hosp_as_of_20200701.parquet"])
        self.cleanup()
