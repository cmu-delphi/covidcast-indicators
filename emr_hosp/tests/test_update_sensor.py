# standard
from copy import deepcopy
import os
from os.path import join, exists
import pytest
from tempfile import TemporaryDirectory

# third party
import pandas as pd
import numpy as np

# third party
from delphi_utils import read_params

# first party
from delphi_emr_hosp.config import Config, Constants
from delphi_emr_hosp.constants import *
from delphi_emr_hosp.update_sensor import write_to_csv, add_prefix, EMRHospSensorUpdator
from delphi_emr_hosp.load_data import *

CONFIG = Config()
CONSTANTS = Constants()
PARAMS = read_params()
CLAIMS_FILEPATH = PARAMS["input_claims_file"]
EMR_FILEPATH = PARAMS["input_emr_file"]
DROP_DATE = pd.to_datetime(PARAMS["drop_date"])
OUTPATH="test_data/"

class TestEMRHospSensorUpdator:
    geo = "hrr"
    parallel = False
    weekday = False
    se = False
    prefix = "foo"
    small_test_data = pd.DataFrame({
        "num": [0, 100, 200, 300, 400, 500, 600, 100, 200, 300, 400, 500, 600],
        "hrr": [1.0] * 7 + [2.0] * 6,
        "den": [1000] * 7 + [2000] * 6,
        "date": [pd.Timestamp(f'03-{i}-2020') for i in range(1, 14)]}).set_index(["hrr","date"])

    def test_shift_dates(self):
        su_inst = EMRHospSensorUpdator(
            "02-01-2020",
            "06-01-2020",
            "06-12-2020",
            self.geo,
            self.parallel,
            self.weekday,
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
        su_inst = EMRHospSensorUpdator(
            "02-01-2020",
            "06-01-2020",
            "06-12-2020",
            'hrr',
            self.parallel,
            self.weekday,
            self.se
        )
        su_inst.shift_dates()
        data_frame = su_inst.geo_reindex(self.small_test_data.reset_index())
        assert data_frame.shape[0] == 2*len(su_inst.fit_dates)
        assert (data_frame.sum() == (4200,19000)).all()

    def test_update_sensor(self):
        for geo in ["state","hrr"]:
            td = TemporaryDirectory()
            su_inst = EMRHospSensorUpdator(
                "02-01-2020",
                "06-01-2020",
                "06-12-2020",
                geo,
                self.parallel,
                self.weekday,
                self.se
            )
            su_inst.update_sensor(
                EMR_FILEPATH,
                CLAIMS_FILEPATH,
                td.name,
                PARAMS["static_file_dir"]
            )
            assert len(os.listdir(td.name)) == len(su_inst.sensor_dates), f"failed {geo} update sensor test"
            td.cleanup()

class TestWriteToCsv:
    def test_write_to_csv_results(self):
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

    def test_handle_wip_signal(self):
        # Test wip_signal = True (all signals should receive prefix)
        signal_names = add_prefix(SIGNALS, True)
        assert all(s.startswith("wip_") for s in signal_names)
        # Test wip_signal = list (only listed signals should receive prefix)
        signal_names = add_prefix(SIGNALS, [SIGNALS[0]])
        assert signal_names[0].startswith("wip_")
        assert all(not s.startswith("wip_") for s in signal_names[1:])
        # Test wip_signal = False (only unpublished signals should receive prefix)
        signal_names = add_prefix(["xyzzy", SIGNALS[0]], False)
        assert signal_names[0].startswith("wip_")
        assert all(not s.startswith("wip_") for s in signal_names[1:])
