# standard
import pytest
import os
import glob
from datetime import datetime

# third party
from delphi_utils import GeoMapper
import pandas as pd

# first party
from delphi_changehc.config import Config
from delphi_changehc.load_data import load_combined_data
from delphi_changehc.backfill import store_backfill_file, merge_backfill_file

CONFIG = Config()
PARAMS = {
    "indicator": {
        "input_denom_file": "test_data/20200601_Counts_Products_Denom.dat.gz",
        "input_covid_file": "test_data/20200601_Counts_Products_Covid.dat.gz",
        "input_flu_file": "test_data/20200601_Counts_Products_Covid.dat.gz",
        "backfill_dir": "./backfill",
        "drop_date": "2020-06-01"
    }
}
COVID_FILEPATH = PARAMS["indicator"]["input_covid_file"]
FLU_FILEPATH = PARAMS["indicator"]["input_flu_file"]
DENOM_FILEPATH = PARAMS["indicator"]["input_denom_file"]
DROP_DATE = pd.to_datetime(PARAMS["indicator"]["drop_date"])
backfill_dir = PARAMS["indicator"]["backfill_dir"]

geo = "county"
weekday = True
backfill_merge_day = 0

combined_data = load_combined_data(DENOM_FILEPATH, COVID_FILEPATH, 
                                       "fips", backfill_dir, geo, weekday, "covid",
                                       backfill_merge_day)

class TestBackfill:
    
    def test_store_backfill_file(self):

        fn = "changehc_covid_as_of_20200101.parquet"
        dropdate = datetime(2020, 1, 1)
        numtype = "covid"
        
        geo = "county"
        weekday = True
        dropdate = datetime(2020, 1, 1)        
        # Check when it cannot be stored
        store_backfill_file(combined_data, dropdate, backfill_dir, numtype, geo, weekday)
        assert fn not in os.listdir(backfill_dir)
        
        geo = "state"
        weekday = False
        dropdate = datetime(2020, 1, 1)        
        # Check when it cannot be stored
        store_backfill_file(combined_data, dropdate, backfill_dir, numtype, geo, weekday)
        assert fn not in os.listdir(backfill_dir)
        
        geo = "county"
        weekday = False
        dropdate = datetime(2020, 1, 1)        
        # Store backfill file
        store_backfill_file(combined_data, dropdate, backfill_dir, numtype, geo, weekday)
        assert fn in os.listdir(backfill_dir)
        fn = "changehc_covid_as_of_20200101.parquet"
        backfill_df = pd.read_parquet(backfill_dir + "/"+ fn, engine='pyarrow')
        
        selected_columns = ['time_value', 'fips', 'state_id',
                        'num', 'den', 'lag', 'issue_date']
        assert set(selected_columns) == set(backfill_df.columns)  
        
        os.remove(backfill_dir + "/" + fn)
        assert fn not in os.listdir(backfill_dir)
        
    def test_merge_backfill_file(self):
        
        geo = "county"
        weekday = False
        numtype = "covid"
        
        today = datetime(2020, 6, 4)
        fn = "changehc_covid_from_20200601_to_20200604.parquet"
        assert fn not in os.listdir(backfill_dir)
        
        merge_backfill_file(backfill_dir, numtype, geo, weekday, today.weekday(),
                            today, test_mode=True, check_nd=2)         
        assert fn not in os.listdir(backfill_dir)
        
        # Generate backfill daily files     
        for d in range(1, 5):
            dropdate = datetime(2020, 6, d)        
            store_backfill_file(combined_data, dropdate, backfill_dir, \
                                numtype, geo, weekday)

        
        # Check the when the merged file is not generated
        today = datetime(2020, 6, 4)
        merge_backfill_file(backfill_dir, numtype, geo, weekday, today.weekday(),
                            today, test_mode=True, check_nd=8)
        assert fn not in os.listdir(backfill_dir)
        
        # Generate the merged file, but not delete it
        merge_backfill_file(backfill_dir, numtype, geo, weekday, today.weekday(),
                            today, test_mode=True, check_nd=2)         
        assert fn in os.listdir(backfill_dir)

        # Read daily file
        new_files = glob.glob(backfill_dir + "/changehc_%s*.parquet"%numtype)
        pdList = []        
        for file in new_files:
            if "from" in file:
                continue
            df = pd.read_parquet(file, engine='pyarrow')
            pdList.append(df)
            os.remove(file)
        new_files = glob.glob(backfill_dir + "/changehc_%s*.parquet"%numtype)
        assert len(new_files) == 1
        
        expected = pd.concat(pdList).sort_values(["time_value", "fips"])
        
        # Read the merged file
        merged = pd.read_parquet(backfill_dir + "/" + fn, engine='pyarrow')
        
        assert set(expected.columns) == set(merged.columns)
        assert expected.shape[0] == merged.shape[0]
        assert expected.shape[1] == merged.shape[1]
        
        os.remove(backfill_dir + "/" + fn)
        assert fn not in os.listdir(backfill_dir)
