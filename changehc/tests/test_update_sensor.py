# standard
import logging
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

# first party
from delphi_changehc.config import Config
from delphi_changehc.update_sensor import write_to_csv, CHCSensorUpdater

CONFIG = Config()
PARAMS = {
    "indicator": {
        "input_denom_file": "test_data/20200601_All_Outpatients_By_County.dat.gz",
        "input_covid_file": "test_data/20200601_Covid_Outpatients_By_County.dat.gz",
        "drop_date": "2020-06-01"
    }
}
COVID_FILEPATH = PARAMS["indicator"]["input_covid_file"]
DENOM_FILEPATH = PARAMS["indicator"]["input_denom_file"]
DROP_DATE = pd.to_datetime(PARAMS["indicator"]["drop_date"])
OUTPATH="test_data/"
TEST_LOGGER = logging.getLogger()

class TestCHCSensorUpdater:
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
        "timestamp": [pd.Timestamp(f'03-{i}-2020') for i in range(1, 14)]}).set_index(["fips","timestamp"])

    def test_shift_dates(self):
        """Tests that dates in the data are shifted according to the burn-in and lag."""
        su_inst = CHCSensorUpdater(
            "02-01-2020",
            "06-01-2020",
            "06-12-2020",
            self.geo,
            self.parallel,
            self.weekday,
            self.numtype,
            self.se,
            "",
            TEST_LOGGER
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
            su_inst = CHCSensorUpdater(
                "02-01-2020",
                "06-01-2020",
                "06-12-2020",
                geo,
                self.parallel,
                self.weekday,
                self.numtype,
                self.se,
                "",
               TEST_LOGGER
            )
            su_inst.shift_dates()
            test_data = pd.DataFrame({
                "num": [0, 100, 200, 300, 400, 500, 600, 100, 200, 300, 400, 500, 600],
                "fips": ['01001'] * 7 + ['04007'] * 6,
                "den": [1000] * 7 + [2000] * 6,
                "timestamp": [pd.Timestamp(f'03-{i}-2020') for i in range(1, 14)]})
            data_frame = su_inst.geo_reindex(test_data)
            assert data_frame.shape[0] == multiple*len(su_inst.fit_dates)
            assert (data_frame.sum() == (4200,19000)).all()

    def test_update_sensor(self):
        """Tests that the sensors are properly updated."""
        outputs = {}
        for geo in ["county", "state", "hhs", "nation"]:
            td = TemporaryDirectory()
            su_inst = CHCSensorUpdater(
                "03-01-2020",
                "03-22-2020",
                "03-27-2020",
                geo,
                self.parallel,
                self.weekday,
                self.numtype,
                self.se,
                "",
                TEST_LOGGER
            )
            # As of 3/3/21 (40c258a), this set of data has county outputting data, state and hhs not
            # outputting data, and nation outputting data, which is undesirable. Ideal behaviour
            # should be all output or a subregion only outputting if its parent has output,
            # which is what is being tested here.
            small_test_data = pd.DataFrame({
                "num": [0, 100, 200, 300, 400, 500, 600, 100, 200, 300, 400, 500, 600] * 2,
                "fips": ["01001"] * 13 + ["42003"] * 13,
                "den": [30, 50, 50, 10, 1, 5, 5, 50, 50, 50, 0, 0, 0] * 2,
                "timestamp": list(pd.date_range("20200301", "20200313")) * 2
            }).set_index(["fips", "timestamp"])
            su_inst.update_sensor(small_test_data,  td.name)
            for f in os.listdir(td.name):
                outputs[f] = pd.read_csv(os.path.join(td.name, f))

            assert len(os.listdir(td.name)) == len(su_inst.sensor_dates),\
                f"failed {geo} update sensor test"
            td.cleanup()
        assert outputs["20200319_county_smoothed_outpatient_covid.csv"].empty
        assert outputs["20200319_state_smoothed_outpatient_covid.csv"].empty
        assert outputs["20200319_hhs_smoothed_outpatient_covid.csv"].empty
        assert outputs["20200319_nation_smoothed_outpatient_covid.csv"].empty

    def test_update_sensor_output_daterange(self):
        """Tests that output does not change when data range changes"""
        small_test_data = pd.DataFrame({
            "num": [0, 100, 200, 300, 400, 500, 600, 100, 200, 300, 400, 500, 600] * 2,
            "fips": ["01001"] * 13 + ["42003"] * 13,
            "den": [30, 50, 50, 10, 1, 5, 5, 50, 50, 50, 0, 0, 0] * 2,
            "timestamp": list(pd.date_range("20200301", "20200313")) * 2
        }).set_index(["fips", "timestamp"])
        startdates = ["2020-03-01", "2020-03-05"]
        outputs = {s:{} for s in startdates}
        for startdate in startdates:
            for geo in ["county", "state", "hhs", "nation"]:
                td = TemporaryDirectory()
                su_inst = CHCSensorUpdater(
                    startdate,
                    "03-22-2020",
                    "03-27-2020",
                    geo,
                    self.parallel,
                    self.weekday,
                    self.numtype,
                    self.se,
                    "",
                    TEST_LOGGER
                )
                su_inst.update_sensor(small_test_data.copy(), td.name)
                for f in os.listdir(td.name):
                    outputs[startdate][f] = pd.read_csv(os.path.join(td.name, f))
            assert len(os.listdir(td.name)) == len(su_inst.sensor_dates),\
                f"failed {geo} update sensor test"
            td.cleanup()

        def pretty(key):
            return "\n".join(f"{s}[{key}]: {len(outputs[s][key])}" for s in startdates)
        for f in outputs[startdates[-1]]:
            assert len(outputs[startdates[0]][f]) == len(outputs[startdates[1]][f]), \
                f"\n{pretty(f)}"
            assert np.array_equal(
                outputs[startdates[0]][f].val.values,
                outputs[startdates[1]][f].val.values
            ), f

class TestWriteToCsv:
    """Tests for writing output files to CSV."""
    def test_write_to_csv_results(self):
        """Tests that the computed data are written correctly."""
        res0 = pd.DataFrame({
            "val": [0.1, 0.5, 1.5] + [1, 2, 3],
            "se": [0.1, 1, 1.1] + [0.5, np.nan, 0.5],
            "sample_size": [np.nan] * 6,
            "timestamp": pd.to_datetime(["2020-05-02", "2020-05-03", "2020-05-05"] * 2),
            "include": [True, True, True] + [True, False, True],
            "geo_id": ["a"] * 3 + ["b"] * 3,
        })

        td = TemporaryDirectory()

        write_to_csv(
            res0[res0['include']],
            geo_level="geography",
            write_se=False,
            day_shift=CONFIG.DAY_SHIFT,
            out_name="name_of_signal",
            output_path=td.name,
            logger=TEST_LOGGER
        )

        # check outputs
        expected_name = "20200502_geography_name_of_signal.csv"
        assert exists(join(td.name, expected_name))
        output_data = pd.read_csv(join(td.name, expected_name))
        expected_columns = ["geo_id", "val", "se", "sample_size"]
        assert (output_data.columns == expected_columns).all()
        assert (output_data.geo_id == ["a", "b"]).all()
        assert np.array_equal(output_data.val.values, np.array([0.1, 1]))

        # for privacy we do not usually report SEs
        assert np.isnan(output_data.se.values).all()
        assert np.isnan(output_data.sample_size.values).all()

        expected_name = "20200503_geography_name_of_signal.csv"
        assert exists(join(td.name, expected_name))
        output_data = pd.read_csv(join(td.name, expected_name))
        assert (output_data.columns == expected_columns).all()
        assert (output_data.geo_id == ["a"]).all()
        assert np.array_equal(output_data.val.values, np.array([0.5]))
        assert np.isnan(output_data.se.values).all()
        assert np.isnan(output_data.sample_size.values).all()

        expected_name = "20200505_geography_name_of_signal.csv"
        assert exists(join(td.name, expected_name))
        output_data = pd.read_csv(join(td.name, expected_name))
        assert (output_data.columns == expected_columns).all()
        assert (output_data.geo_id == ["a", "b"]).all()
        assert np.array_equal(output_data.val.values, np.array([1.5, 3]))
        assert np.isnan(output_data.se.values).all()
        assert np.isnan(output_data.sample_size.values).all()

        td.cleanup()

    def test_write_to_csv_with_se_results(self):
        """Tests that the standard error is written when requested."""
        res0 = pd.DataFrame({
            "val": [0.1, 0.5, 1.5] + [1, 2, 3],
            "se": [0.1, 1, 1.1] + [0.5, np.nan, 0.5],
            "sample_size": [np.nan] * 6,
            "timestamp": pd.to_datetime(["2020-05-02", "2020-05-03", "2020-05-05"] * 2),
            "include": [True, True, True] + [True, False, True],
            "geo_id": ["a"] * 3 + ["b"] * 3,
        })

        td = TemporaryDirectory()
        write_to_csv(
            res0[res0['include']],
            geo_level="geography",
            write_se=True,
            day_shift=CONFIG.DAY_SHIFT,
            out_name="name_of_signal",
            output_path=td.name,
            logger=TEST_LOGGER
        )

        # check outputs
        expected_name = "20200502_geography_name_of_signal.csv"
        assert exists(join(td.name, expected_name))
        output_data = pd.read_csv(join(td.name, expected_name))
        expected_columns = ["geo_id", "val", "se", "sample_size"]
        assert (output_data.columns == expected_columns).all()
        assert (output_data.geo_id == ["a", "b"]).all()
        assert np.array_equal(output_data.val.values, np.array([0.1, 1]))
        assert np.array_equal(output_data.se.values, np.array([0.1, 0.5]))
        assert np.isnan(output_data.sample_size.values).all()
        td.cleanup()

    def test_write_to_csv_wrong_results(self):
        """Tests that nonsensical inputs trigger exceptions."""
        res0 = pd.DataFrame({
            "val": [0.1, 0.5, 1.5] + [1, 2, 3],
            "se": [0.1, 1, 1.1] + [0.5, 0.5, 0.5],
            "sample_size": [np.nan] * 6,
            "timestamp": pd.to_datetime(["2020-05-02", "2020-05-03", "2020-05-05"] * 2),
            "include": [True, True, True] + [True, False, True],
            "geo_id": ["a"] * 3 + ["b"] * 3,
        }).set_index(["timestamp", "geo_id"]).sort_index()

        td = TemporaryDirectory()

        # nan value for included loc-date
        res1 = res0.copy()
        res1 = res1[res1['include']]
        res1.loc[("2020-05-02", "a"), "val"] = np.nan
        res1.reset_index(inplace=True)
        with pytest.raises(AssertionError):
            write_to_csv(
                res1,
                geo_level="geography",
                write_se=False,
                day_shift=CONFIG.DAY_SHIFT,
                out_name="name_of_signal",
                output_path=td.name,
                logger=TEST_LOGGER
            )

        # nan se for included loc-date
        res2 = res0.copy()
        res2 = res2[res2['include']]
        res2.loc[("2020-05-02", "a"), "se"] = np.nan
        res2.reset_index(inplace=True)
        with pytest.raises(AssertionError):
            write_to_csv(
                res2,
                geo_level="geography",
                write_se=True,
                day_shift=CONFIG.DAY_SHIFT,
                out_name="name_of_signal",
                output_path=td.name,
                logger=TEST_LOGGER
            )

        # large se value
        res3 = res0.copy()
        res3 = res3[res3['include']]
        res3.loc[("2020-05-02", "a"), "se"] = 10
        res3.reset_index(inplace=True)
        with pytest.raises(AssertionError):
            write_to_csv(
                res3,
                geo_level="geography",
                write_se=True,
                day_shift=CONFIG.DAY_SHIFT,
                out_name="name_of_signal",
                output_path=td.name,
                logger=TEST_LOGGER
            )

        td.cleanup()
