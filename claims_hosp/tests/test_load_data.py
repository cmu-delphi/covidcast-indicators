# third party
import pandas as pd
import pytest

# first party
from delphi_claims_hosp.config import Config, GeoConstants
from delphi_claims_hosp.load_data import load_data, load_claims_data

CONFIG = Config()
CONSTANTS = GeoConstants()
PARAMS = {
    "indicator": {
        "input_file": "test_data/SYNEDI_AGG_INPATIENT_11062020_1451CDT.csv.gz",
        "drop_date": "2020-06-11",
    }
}
DATA_FILEPATH = PARAMS["indicator"]["input_file"]
DROP_DATE = pd.to_datetime(PARAMS["indicator"]["drop_date"])


class TestLoadData:
    fips_claims_data = load_claims_data(DATA_FILEPATH, DROP_DATE, "fips")
    hrr_claims_data = load_claims_data(DATA_FILEPATH, DROP_DATE, "hrr")
    fips_data = load_data(DATA_FILEPATH, DROP_DATE, "fips")
    hrr_data = load_data(DATA_FILEPATH, DROP_DATE, "hrr")

    def test_base_unit(self):
        with pytest.raises(AssertionError):
            load_claims_data(DATA_FILEPATH, DROP_DATE, "foo")

        with pytest.raises(AssertionError):
            load_data(DATA_FILEPATH, DROP_DATE, "foo")

    def test_claims_columns(self):
        assert "hrr" in self.hrr_claims_data.index.names
        assert "fips" in self.fips_claims_data.index.names
        assert "date" in self.hrr_claims_data.index.names
        assert "date" in self.fips_claims_data.index.names

        expected_claims_columns = ["Denominator", "Covid_like"]
        for col in expected_claims_columns:
            assert col in self.fips_claims_data.columns
            assert col in self.hrr_claims_data.columns
        assert len(set(self.fips_claims_data.columns) - set(expected_claims_columns)) == 0
        assert len(set(self.hrr_claims_data.columns) - set(expected_claims_columns)) == 0

    def test_data_columns(self):
        assert "hrr" in self.hrr_data.columns
        assert "fips" in self.fips_data.columns
        assert "date" in self.hrr_data.columns
        assert "date" in self.fips_data.columns

        expected_columns = ["num", "den"]
        for col in expected_columns:
            assert col in self.fips_data.columns
            assert col in self.hrr_data.columns

    def test_edge_values(self):
        for data in [self.hrr_claims_data, self.fips_claims_data]:
            assert data.index.get_level_values('date').max() >= Config.FIRST_DATA_DATE
            assert data.index.get_level_values('date').min() < DROP_DATE

        for data in [self.hrr_data, self.fips_data]:
            assert data.date.max() >= Config.FIRST_DATA_DATE
            assert data.date.min() < DROP_DATE

    def test_hrrs_values(self):
        assert len(self.hrr_data.hrr.unique()) <= CONSTANTS.NUM_HRRS
        assert len(self.hrr_claims_data.index.get_level_values(
            'hrr').unique()) <= CONSTANTS.NUM_HRRS
        assert self.hrr_data.isna().sum().sum() == 0
        assert self.hrr_data["num"].sum() == self.hrr_claims_data["Covid_like"].sum()
        assert self.hrr_data["den"].sum() == self.hrr_claims_data["Denominator"].sum()

    def test_fips_values(self):
        assert len(self.fips_data.fips.unique()) <= CONSTANTS.NUM_COUNTIES
        assert len(self.fips_claims_data.index.get_level_values(
            'fips').unique()) <= CONSTANTS.NUM_COUNTIES
        assert self.fips_data.isna().sum().sum() == 0
        assert self.fips_data["num"].sum() == self.fips_claims_data["Covid_like"].sum()
        assert self.fips_data["den"].sum() == self.fips_claims_data["Denominator"].sum()
