import os
import glob
from datetime import datetime

# third party
import pandas as pd
import pytest

# first party
from delphi_claims_hosp.config import Config, GeoConstants
from delphi_claims_hosp.backfill import store_backfill_file, merge_backfill_file

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
        store_backfill_file(DATA_FILEPATH, dropdate, backfill_dir)
        assert fn in os.listdir(backfill_dir)
        fn = "claims_hosp_as_of_20200101.parquet"
        backfill_df = pd.read_parquet(backfill_dir + "/"+ fn, engine='pyarrow')
        
        selected_columns = ['time_value', 'fips', 'state_id',
                        'num', 'den']
        assert set(selected_columns) == set(backfill_df.columns)  
        
        os.remove(backfill_dir + "/" + fn)
        assert fn not in os.listdir(backfill_dir)
        
    def test_merge_backfill_file(self):
        
        today = datetime.today()
        
        fn = "claims_hosp_from_20200611_to_20200614.parquet"
        assert fn not in os.listdir(backfill_dir)
        
        # Check when there is no daily file to merge.
        today = datetime(2020, 6, 14)
        merge_backfill_file(backfill_dir, today.weekday(), today, 
                            test_mode=True, check_nd=8)
        assert fn not in os.listdir(backfill_dir)
        
        # Generate backfill daily files     
        for d in range(11, 15):
            dropdate = datetime(2020, 6, d)        
            store_backfill_file(DATA_FILEPATH, dropdate, backfill_dir)
        
        # Check the when the merged file is not generated
        today = datetime(2020, 6, 14)
        merge_backfill_file(backfill_dir, today.weekday(), today, 
                            test_mode=True, check_nd=8)
        assert fn not in os.listdir(backfill_dir)
        
         # Generate the merged file, but not delete it
        merge_backfill_file(backfill_dir, today.weekday(), today, 
                            test_mode=True, check_nd=2)         
        assert fn in os.listdir(backfill_dir)

        # Read daily file
        new_files = glob.glob(backfill_dir + "/claims_hosp*.parquet")
        pdList = []        
        for file in new_files:
            if "from" in file:
                continue
            df = pd.read_parquet(file, engine='pyarrow')
            issue_date = datetime.strptime(file[-16:-8], "%Y%m%d")
            df["issue_date"] = issue_date
            df["lag"] = [(issue_date - x).days for x in df["time_value"]]
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
