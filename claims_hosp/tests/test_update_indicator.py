# standard
import os
from copy import deepcopy
from os.path import join, exists
from tempfile import TemporaryDirectory

# third party
from delphi_utils import read_params
import numpy as np
import pandas as pd
import pytest

# first party
from delphi_claims_hosp.config import Config, GeoConstants
from delphi_claims_hosp.update_indicator import ClaimsHospIndicatorUpdater

CONFIG = Config()
CONSTANTS = GeoConstants()
PARAMS = read_params()
DATA_FILEPATH = PARAMS["input_file"]
DROP_DATE = pd.to_datetime(PARAMS["drop_date"])
OUTPATH = "test_data/"


class TestClaimsHospIndicatorUpdater:
    geo = "hrr"
    parallel = False
    weekday = False
    write_se = False
    prefix = "foo"
    small_test_data = pd.DataFrame({
        "num": [0, 100, 200, 300, 400, 500, 600, 100, 200, 300, 400, 500, 600],
        "hrr": [1.0] * 7 + [2.0] * 6,
        "den": [1000] * 7 + [2000] * 6,
        "date": [pd.Timestamp(f'03-{i}-2020') for i in range(1, 14)]}).set_index(
        ["hrr", "date"])

    def test_shift_dates(self):
        updater = ClaimsHospIndicatorUpdater(
            "02-01-2020",
            "06-01-2020",
            "06-12-2020",
            self.geo,
            self.parallel,
            self.weekday,
            self.write_se,
            Config.signal_name
        )
        ## Test init
        assert updater.startdate.month == 2
        assert updater.enddate.month == 6
        assert updater.dropdate.day == 12

        ## Test shift
        updater.shift_dates()
        assert updater.output_dates[0] == updater.startdate
        assert updater.output_dates[-1] == updater.enddate - pd.Timedelta(days=1)

    def test_geo_reindex(self):
        updater = ClaimsHospIndicatorUpdater(
            "02-01-2020",
            "06-01-2020",
            "06-12-2020",
            self.geo,
            self.parallel,
            self.weekday,
            self.write_se,
            Config.signal_name
        )
        updater.shift_dates()
        data_frame = updater.geo_reindex(self.small_test_data.reset_index())
        assert data_frame.shape[0] == 2 * len(updater.fit_dates)
        assert (data_frame.sum() == (4200, 19000)).all()

    def test_update_indicator(self):
        for geo in ["state", "hrr", "hhs", "nation"]:
            td = TemporaryDirectory()
            updater = ClaimsHospIndicatorUpdater(
                "02-01-2020",
                "06-01-2020",
                "06-12-2020",
                geo,
                self.parallel,
                self.weekday,
                self.write_se,
                Config.signal_name
            )

            updater.update_indicator(
                DATA_FILEPATH,
                td.name
            )

            assert len(os.listdir(td.name)) == len(
                updater.output_dates), f"failed {geo} update_indicator test"
            td.cleanup()

    def test_write_to_csv_results(self):
        updater = ClaimsHospIndicatorUpdater(
            "02-01-2020",
            "06-01-2020",
            "06-12-2020",
            self.geo,
            self.parallel,
            self.weekday,
            self.write_se,
            Config.signal_name
        )

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
        updater.write_to_csv(res0, td.name)

        # check outputs
        expected_name = f"20200502_geography_{Config.signal_name}.csv"
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

        expected_name = f"20200503_geography_{Config.signal_name}.csv"
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

        expected_name = f"20200505_geography_{Config.signal_name}.csv"
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
        obfuscated_name = PARAMS["obfuscated_prefix"]
        signal_name = obfuscated_name + "_" + Config.signal_weekday_name
        updater = ClaimsHospIndicatorUpdater(
            "02-01-2020",
            "06-01-2020",
            "06-12-2020",
            self.geo,
            self.parallel,
            True,
            True,
            signal_name
        )

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
        updater.write_to_csv(res0, td.name)

        # check outputs
        expected_name = f"20200502_geography_{signal_name}.csv"
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
        updater = ClaimsHospIndicatorUpdater(
            "02-01-2020",
            "06-01-2020",
            "06-12-2020",
            self.geo,
            self.parallel,
            self.weekday,
            self.write_se,
            Config.signal_name
        )

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
            updater.write_to_csv(res1, td.name)

        # nan se for included loc-date
        res2 = deepcopy(res0)
        res2["se"]["a"][1] = np.nan
        with pytest.raises(AssertionError):
            updater.write_to_csv(res2, td.name)

        # large se value
        res3 = deepcopy(res0)
        res3["se"]["a"][0] = 10
        with pytest.raises(AssertionError):
            updater.write_to_csv(res3, td.name)

        td.cleanup()
