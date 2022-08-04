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
from delphi_changehc.load_data import *

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

class TestLoadData:
    denom_data = load_chng_data(DENOM_FILEPATH, DROP_DATE, "fips",
                    Config.DENOM_COLS, Config.DENOM_DTYPES, Config.DENOM_COL)
    covid_data = load_chng_data(COVID_FILEPATH, DROP_DATE, "fips",
                    Config.COVID_COLS, Config.COVID_DTYPES, Config.COVID_COL)
    combined_data = load_combined_data(DENOM_FILEPATH, COVID_FILEPATH, DROP_DATE,
                                       "fips", backfill_dir, geo, weekday, "covid",
                                       backfill_merge_day)
    flu_data = load_flu_data(DENOM_FILEPATH, FLU_FILEPATH, DROP_DATE,"fips",
                             backfill_dir, geo, weekday, "flu", backfill_merge_day)
    gmpr = GeoMapper()

    def test_base_unit(self):
        with pytest.raises(AssertionError):
            load_chng_data(DENOM_FILEPATH, DROP_DATE, "foo",
                    Config.DENOM_COLS, Config.DENOM_DTYPES, Config.DENOM_COL)

        with pytest.raises(AssertionError):
            load_chng_data(DENOM_FILEPATH, DROP_DATE, "fips",
                    Config.DENOM_COLS, Config.DENOM_DTYPES, Config.COVID_COL)

        with pytest.raises(AssertionError):
            load_combined_data(DENOM_FILEPATH, COVID_FILEPATH, DROP_DATE, "foo", 
                               backfill_dir, geo, weekday, "covid", backfill_merge_day)

        with pytest.raises(AssertionError):
            load_flu_data(DENOM_FILEPATH, FLU_FILEPATH, DROP_DATE, "foo", 
                          backfill_dir, geo, weekday, "covid", backfill_merge_day)

    def test_denom_columns(self):
        assert "fips" in self.denom_data.index.names
        assert "timestamp" in self.denom_data.index.names

        expected_denom_columns = ["Denominator"]
        for col in expected_denom_columns:
            assert col in self.denom_data.columns
        assert len(set(self.denom_data.columns) - set(expected_denom_columns)) == 0

    def test_claims_columns(self):
        assert "fips" in self.covid_data.index.names
        assert "timestamp" in self.covid_data.index.names

        expected_covid_columns = ["COVID"]
        for col in expected_covid_columns:
            assert col in self.covid_data.columns
        assert len(set(self.covid_data.columns) - set(expected_covid_columns)) == 0

    def test_combined_columns(self):
        assert "fips" in self.combined_data.index.names
        assert "timestamp" in self.combined_data.index.names

        expected_combined_columns = ["num", "den"]
        for col in expected_combined_columns:
            assert col in self.combined_data.columns
        assert len(
            set(self.combined_data.columns) - set(expected_combined_columns)) == 0

    def test_flu_columns(self):
        assert "fips" in self.flu_data.index.names
        assert "timestamp" in self.flu_data.index.names

        expected_flu_columns = ["num", "den"]
        for col in expected_flu_columns:
            assert col in self.flu_data.columns
        assert len(
            set(self.flu_data.columns) - set(expected_flu_columns)) == 0

    def test_edge_values(self):
        for data in [self.denom_data,
                     self.covid_data,
                     self.combined_data]:
            assert data.index.get_level_values("timestamp").max() >= Config.FIRST_DATA_DATE
            assert data.index.get_level_values("timestamp").min() < DROP_DATE

    def test_fips_values(self):
        for data in [self.denom_data,
                     self.covid_data,
                     self.combined_data]:
            assert (
                    len(data.index.get_level_values(
                        "fips").unique()) <= len(self.gmpr.get_geo_values("fips"))
            )

    def test_combined_fips_values(self):
        assert self.combined_data.isna().sum().sum() == 0

        sum_fips_num = (
                self.covid_data["COVID"].sum()
        )
        sum_fips_den = (
                self.denom_data["Denominator"].sum()
        )

        assert self.combined_data["num"].sum() == sum_fips_num
        assert self.combined_data["den"].sum() == sum_fips_den
    
    def store_backfill_file(self):
        
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
        
        selected_columns = ['time_value', 'fips',
                        'num', 'den']
        assert set(selected_columns) == set(backfill_df.columns)  
        
        os.remove(backfill_dir + "/" + fn)
        assert fn not in os.listdir(backfill_dir)
        
    def test_merge_backfill_file(self):
        
        today = datetime.today()
        geo = "county"
        weekday = False
        numtype = "covid"
        
        new_files = glob.glob(backfill_dir + "/changehc_%s*.parquet"%numtype)
        fn = "changehc_covid_from_20200601_to_20200604.parquet"
        assert fn not in os.listdir(backfill_dir)
        
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
