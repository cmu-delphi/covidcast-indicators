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
    combined_data = load_combined_data(DENOM_FILEPATH, COVID_FILEPATH, 
                                       "fips", backfill_dir, geo, weekday, "covid",
                                       True, backfill_merge_day)
    flu_data = load_flu_data(DENOM_FILEPATH, FLU_FILEPATH, "fips",
                             backfill_dir, geo, weekday, "flu", True, backfill_merge_day)
    gmpr = GeoMapper()

    def test_base_unit(self):
        with pytest.raises(AssertionError):
            load_chng_data(DENOM_FILEPATH, DROP_DATE, "foo",
                    Config.DENOM_COLS, Config.DENOM_DTYPES, Config.DENOM_COL)

        with pytest.raises(AssertionError):
            load_chng_data(DENOM_FILEPATH, DROP_DATE, "fips",
                    Config.DENOM_COLS, Config.DENOM_DTYPES, Config.COVID_COL)

        with pytest.raises(AssertionError):
            load_combined_data(DENOM_FILEPATH, COVID_FILEPATH, "foo", 
                               backfill_dir, geo, weekday, "covid", True, backfill_merge_day)

        with pytest.raises(AssertionError):
            load_flu_data(DENOM_FILEPATH, FLU_FILEPATH, "foo", 
                          backfill_dir, geo, weekday, "covid", True, backfill_merge_day)

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
