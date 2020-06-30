# standard
import pytest

# third party
from delphi_utils import read_params
import pandas as pd

# first party
from delphi_emr_hosp.config import Config, Constants
from delphi_emr_hosp.load_data import *

CONFIG = Config()
CONSTANTS = Constants()
PARAMS = read_params()
CLAIMS_FILEPATH = PARAMS["input_claims_file"]
EMR_FILEPATH = PARAMS["input_emr_file"]
DROP_DATE = pd.to_datetime(PARAMS["drop_date"])


class TestLoadData:
    fips_emr_data = load_emr_data(EMR_FILEPATH, DROP_DATE, "fips")
    hrr_emr_data = load_emr_data(EMR_FILEPATH, DROP_DATE, "hrr")
    fips_claims_data = load_claims_data(CLAIMS_FILEPATH, DROP_DATE, "fips")
    hrr_claims_data = load_claims_data(CLAIMS_FILEPATH, DROP_DATE, "hrr")
    fips_combined_data = load_combined_data(EMR_FILEPATH, CLAIMS_FILEPATH, DROP_DATE,
                                            "fips")
    hrr_combined_data = load_combined_data(EMR_FILEPATH, CLAIMS_FILEPATH, DROP_DATE,
                                           "hrr")

    def test_base_unit(self):
        with pytest.raises(AssertionError):
            load_emr_data(EMR_FILEPATH, DROP_DATE, "foo")

        with pytest.raises(AssertionError):
            load_claims_data(CLAIMS_FILEPATH, DROP_DATE, "foo")

        with pytest.raises(AssertionError):
            load_combined_data(EMR_FILEPATH, CLAIMS_FILEPATH, DROP_DATE, "foo")

    def test_emr_columns(self):
        assert "hrr" in self.hrr_emr_data.index.names
        assert "fips" in self.fips_emr_data.index.names
        assert "date" in self.hrr_emr_data.index.names
        assert "date" in self.fips_emr_data.index.names

        expected_emr_columns = ["Total_Count", "IP_COVID_Total_Count"]
        for col in expected_emr_columns:
            assert col in self.fips_emr_data.columns
            assert col in self.hrr_emr_data.columns
        assert len(set(self.fips_emr_data.columns) - set(expected_emr_columns)) == 0
        assert len(set(self.hrr_emr_data.columns) - set(expected_emr_columns)) == 0

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

    def test_combined_columns(self):
        assert "hrr" in self.hrr_combined_data.index.names
        assert "fips" in self.fips_combined_data.index.names
        assert "date" in self.hrr_combined_data.index.names
        assert "date" in self.fips_combined_data.index.names

        expected_combined_columns = ["num", "den"]
        for col in expected_combined_columns:
            assert col in self.fips_combined_data.columns
            assert col in self.hrr_combined_data.columns
        assert len(
            set(self.fips_combined_data.columns) - set(expected_combined_columns)) == 0
        assert len(
            set(self.hrr_combined_data.columns) - set(expected_combined_columns)) == 0

    def test_edge_values(self):
        for data in [self.hrr_emr_data,
                     self.hrr_claims_data,
                     self.hrr_combined_data,
                     self.fips_emr_data,
                     self.fips_claims_data,
                     self.fips_combined_data]:
            assert data.index.get_level_values('date').max() >= Config.FIRST_DATA_DATE
            assert data.index.get_level_values('date').min() < DROP_DATE

    def test_hrrs_values(self):
        for data in [self.hrr_emr_data,
                     self.hrr_claims_data,
                     self.hrr_combined_data]:
            assert len(data.index.get_level_values('hrr').unique()) <= CONSTANTS.NUM_HRRS

    def test_fips_values(self):
        for data in [self.fips_emr_data,
                     self.fips_claims_data,
                     self.fips_combined_data]:
            assert (
                    len(data.index.get_level_values(
                        'fips').unique()) <= CONSTANTS.NUM_COUNTIES
            )

    def test_combined_fips_values(self):
        assert self.fips_combined_data.isna().sum().sum() == 0

        sum_fips_num = (
                self.fips_emr_data["IP_COVID_Total_Count"].sum() +
                self.fips_claims_data["Covid_like"].sum()
        )
        sum_fips_den = (
                self.fips_emr_data["Total_Count"].sum() +
                self.fips_claims_data["Denominator"].sum()
        )

        assert self.fips_combined_data["num"].sum() == sum_fips_num
        assert self.fips_combined_data["den"].sum() == sum_fips_den

    def test_combined_hrr_values(self):
        assert self.hrr_combined_data.isna().sum().sum() == 0
        sum_hrr_num = (
                self.hrr_emr_data["IP_COVID_Total_Count"].sum() +
                self.hrr_claims_data["Covid_like"].sum()
        )
        sum_hrr_den = (
                self.hrr_emr_data["Total_Count"].sum() +
                self.hrr_claims_data["Denominator"].sum()
        )

        assert self.hrr_combined_data["num"].sum() == sum_hrr_num
        assert self.hrr_combined_data["den"].sum() == sum_hrr_den
