# standard
from copy import deepcopy
import os
from os.path import join, exists
from tempfile import TemporaryDirectory

# third party
import pandas as pd
import numpy as np
from boto3 import Session
from moto import mock_s3
import pytest

# third party
from delphi_utils import read_params

# first party
from delphi_changehc.config import Config, Constants
from delphi_changehc.update_sensor import write_to_csv, CHCSensorUpdator

CONFIG = Config()
CONSTANTS = Constants()
PARAMS = read_params()
COVID_FILEPATH = PARAMS["input_covid_file"]
DENOM_FILEPATH = PARAMS["input_denom_file"]
DROP_DATE = pd.to_datetime(PARAMS["drop_date"])
OUTPATH="test_data/"

class TestCHCSensorUpdator:
    """Tests for updating the sensors."""
    geo = "county"
    parallel = False
    weekday = False
    numtype = "covid"
    se = False
    prefix = "foo"
    small_test_data = pd.DataFrame({
        "num": [0, 100, 200, 300, 400, 500, 600, 100, 200, 300, 400, 500, 600],
        "fips": ['01001'] * 7 + ['04007'] * 6,
        "den": [1000] * 7 + [2000] * 6,
        "date": [pd.Timestamp(f'03-{i}-2020') for i in range(1, 14)]}).set_index(["fips","date"])

    def test_shift_dates(self):
        """Tests that dates in the data are shifted according to the burn-in and lag."""
        su_inst = CHCSensorUpdator(
            "02-01-2020",
            "06-01-2020",
            "06-12-2020",
            self.geo,
            self.parallel,
            self.weekday,
            self.numtype,
            self.se
        )
        ## Test init
        assert su_inst.startdate.month == 2
        assert su_inst.enddate.month == 6
        assert su_inst.dropdate.day == 12

        ## Test shift
        su_inst.shift_dates()
        assert su_inst.sensor_dates[0] == su_inst.startdate
        assert su_inst.sensor_dates[-1] == su_inst.enddate - pd.Timedelta(days=1)

    def test_geo_reindex(self):
        """Tests that the geo reindexer changes the geographic resolution."""
        for geo, multiple in [("nation", 1), ("county", 2), ("hhs", 2)]:
            su_inst = CHCSensorUpdator(
                "02-01-2020",
                "06-01-2020",
                "06-12-2020",
                geo,
                self.parallel,
                self.weekday,
                self.numtype,
                self.se
            )
            su_inst.shift_dates()
            data_frame = su_inst.geo_reindex(self.small_test_data.reset_index())
            assert data_frame.shape[0] == multiple*len(su_inst.fit_dates)
            assert (data_frame.sum() == (4200,19000)).all()

    def test_update_sensor(self):
        """Tests that the sensors are properly updated."""
        for geo in ["state","hrr"]:
            td = TemporaryDirectory()
            su_inst = CHCSensorUpdator(
                "02-01-2020",
                "06-01-2020",
                "06-12-2020",
                geo,
                self.parallel,
                self.weekday,
                self.numtype,
                self.se
            )

            with mock_s3():
                # Create the fake bucket we will be using
                params = read_params()
                aws_credentials = params["aws_credentials"]
                s3_client = Session(**aws_credentials).client("s3")
                s3_client.create_bucket(Bucket=params["bucket_name"])
                su_inst.update_sensor(
                    self.small_test_data,
                    td.name)

            assert len(os.listdir(td.name)) == len(su_inst.sensor_dates),\
                f"failed {geo} update sensor test"
            td.cleanup()

class TestWriteToCsv:
    """Tests for writing output files to CSV."""
    def test_write_to_csv_results(self):
        """Tests that the computed data are written correctly."""
        res0 = {
            "rates": {
                "a": [0.1, 0.5, 1.5],
                "b": [1, 2, 3]
            },
            "se": {
                "a": [0.1, 1, 1.1],
                "b": [0.5, np.nan, 0.5]
            },
            "dates": [
                pd.to_datetime("2020-05-01"),
                pd.to_datetime("2020-05-02"),
                pd.to_datetime("2020-05-04")
            ],
            "include": {
                "a": [True, True, True],
                "b": [True, False, True]
            },
            "geo_ids": ["a", "b"],
            "geo_level": "geography",
        }

        td = TemporaryDirectory()
        write_to_csv(res0, False, "name_of_signal", td.name)

        # check outputs
        expected_name = "20200502_geography_name_of_signal.csv"
        assert exists(join(td.name, expected_name))
        output_data = pd.read_csv(join(td.name, expected_name))
        assert (
                output_data.columns == ["geo_id", "val", "se", "direction", "sample_size"]
        ).all()
        assert (output_data.geo_id == ["a", "b"]).all()
        assert np.array_equal(output_data.val.values, np.array([0.1, 1]))

        # for privacy we do not usually report SEs
        assert np.isnan(output_data.se.values).all()
        assert np.isnan(output_data.direction.values).all()
        assert np.isnan(output_data.sample_size.values).all()

        expected_name = "20200503_geography_name_of_signal.csv"
        assert exists(join(td.name, expected_name))
        output_data = pd.read_csv(join(td.name, expected_name))
        assert (
                output_data.columns == ["geo_id", "val", "se", "direction", "sample_size"]
        ).all()
        assert (output_data.geo_id == ["a"]).all()
        assert np.array_equal(output_data.val.values, np.array([0.5]))
        assert np.isnan(output_data.se.values).all()
        assert np.isnan(output_data.direction.values).all()
        assert np.isnan(output_data.sample_size.values).all()

        expected_name = "20200505_geography_name_of_signal.csv"
        assert exists(join(td.name, expected_name))
        output_data = pd.read_csv(join(td.name, expected_name))
        assert (
                output_data.columns == ["geo_id", "val", "se", "direction", "sample_size"]
        ).all()
        assert (output_data.geo_id == ["a", "b"]).all()
        assert np.array_equal(output_data.val.values, np.array([1.5, 3]))
        assert np.isnan(output_data.se.values).all()
        assert np.isnan(output_data.direction.values).all()
        assert np.isnan(output_data.sample_size.values).all()

        td.cleanup()

    def test_write_to_csv_with_se_results(self):
        """Tests that the standard error is written when requested."""
        res0 = {
            "rates": {
                "a": [0.1, 0.5, 1.5],
                "b": [1, 2, 3]
            },
            "se": {
                "a": [0.1, 1, 1.1],
                "b": [0.5, np.nan, 0.5]
            },
            "dates": [
                pd.to_datetime("2020-05-01"),
                pd.to_datetime("2020-05-02"),
                pd.to_datetime("2020-05-04")
            ],
            "include": {
                "a": [True, True, True],
                "b": [True, False, True]
            },
            "geo_ids": ["a", "b"],
            "geo_level": "geography",
        }

        td = TemporaryDirectory()
        write_to_csv(res0, True, "name_of_signal", td.name)

        # check outputs
        expected_name = "20200502_geography_name_of_signal.csv"
        assert exists(join(td.name, expected_name))
        output_data = pd.read_csv(join(td.name, expected_name))
        assert (
                output_data.columns == ["geo_id", "val", "se", "direction", "sample_size"]
        ).all()
        assert (output_data.geo_id == ["a", "b"]).all()
        assert np.array_equal(output_data.val.values, np.array([0.1, 1]))
        assert np.array_equal(output_data.se.values, np.array([0.1, 0.5]))
        assert np.isnan(output_data.direction.values).all()
        assert np.isnan(output_data.sample_size.values).all()
        td.cleanup()

    def test_write_to_csv_wrong_results(self):
        """Tests that nonsensical inputs trigger exceptions."""
        res0 = {
            "rates": {
                "a": [0.1, 0.5, 1.5],
                "b": [1, 2, 3]
            },
            "se": {
                "a": [0.1, 1, 1.1],
                "b": [0.5, 0.5, 0.5]
            },
            "dates": [
                pd.to_datetime("2020-05-01"),
                pd.to_datetime("2020-05-02"),
                pd.to_datetime("2020-05-04")
            ],
            "include": {
                "a": [True, True, True],
                "b": [True, False, True]
            },
            "geo_ids": ["a", "b"],
            "geo_level": "geography",
        }

        td = TemporaryDirectory()

        # nan value for included loc-date
        res1 = deepcopy(res0)
        res1["rates"]["a"][1] = np.nan
        with pytest.raises(AssertionError):
            write_to_csv(res1, False, "name_of_signal", td.name)

        # nan se for included loc-date
        res2 = deepcopy(res0)
        res2["se"]["a"][1] = np.nan
        with pytest.raises(AssertionError):
            write_to_csv(res2, False, "name_of_signal", td.name)

        # large se value
        res3 = deepcopy(res0)
        res3["se"]["a"][0] = 10
        with pytest.raises(AssertionError):
            write_to_csv(res3, False, "name_of_signal", td.name)

        td.cleanup()
