# standard
import os
from copy import deepcopy
from os.path import join, exists
from tempfile import TemporaryDirectory

# third party
import numpy as np
import pandas as pd
import logging
import pytest

# first party
from delphi_claims_hosp.config import Config, GeoConstants
from delphi_claims_hosp.update_indicator import ClaimsHospIndicatorUpdater
from delphi_utils.export import create_export_csv

CONFIG = Config()
CONSTANTS = GeoConstants()
PARAMS = {
    "indicator": {
        "input_file": "test_data/SYNEDI_AGG_INPATIENT_11062020_1451CDT.csv.gz",
        "drop_date": "2020-06-11",
        "obfuscated_prefix": "foo_obfuscated"
    }
}
DATA_FILEPATH = PARAMS["indicator"]["input_file"]
DROP_DATE = pd.to_datetime(PARAMS["indicator"]["drop_date"])
OUTPATH = "test_data/"
TEST_LOGGER = logging.getLogger()


class TestClaimsHospIndicatorUpdater:
    geo = "hrr"
    parallel = False
    weekday = False
    write_se = False
    prefix = "foo"
    start_date = "02-01-2020"
    end_date = "06-01-2020"
    drop_date = "06-12-2020"
    small_test_data = pd.DataFrame({
        "num": [0, 100, 200, 300, 400, 500, 600, 100, 200, 300, 400, 500, 600],
        "hrr": [1.0] * 7 + [2.0] * 6,
        "den": [1000] * 7 + [2000] * 6,
        "timestamp": [pd.Timestamp(f'03-{i}-2020') for i in range(1, 14)]}).set_index(
        ["hrr", "timestamp"])

    def test_shift_dates(self):
        updater = ClaimsHospIndicatorUpdater(
            self.start_date,
            self.end_date,
            self.drop_date,
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
            self.start_date,
            self.end_date,
            self.drop_date,
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
                self.start_date,
                self.end_date,
                self.drop_date,
                geo,
                self.parallel,
                self.weekday,
                self.write_se,
                Config.signal_name
            )

            output = updater.update_indicator(
                DATA_FILEPATH,
                TEST_LOGGER
            )

            filtered_output = updater.preprocess_output(output)
            create_export_csv(filtered_output,
                              td.name,
                              start_date=self.start_date,
                              end_date=self.end_date,
                              geo_res=geo,
                              write_empty_days=True,
                              sensor=Config.signal_name)
            # output date range is half exclusive [2020-02-01 to 2020-06-01)
            # while the export function considers fully inclusive [2020-02-01. 2020-06-01]
            assert len(os.listdir(td.name)) == len(
                updater.output_dates) + 1, f"failed {geo} update_indicator test"
            td.cleanup()

    def prepare_df(self, d):
        df_list = []
        for geo in d.get("geo_ids"):
           df_list.append(pd.DataFrame({"geo_id": geo,"rate": d["rates"][geo], "se": d["se"][geo],
                                       "incl": d["include"][geo], "timestamp": d["dates"],
                                        "sample_size": [np.nan, np.nan, np.nan]
                                        }))

        output_df = pd.concat(df_list)
        output_df.index = output_df.timestamp
        output_df.drop(columns=["timestamp"], inplace=True)
        return output_df
    def test_write_to_csv_results(self):
        updater = ClaimsHospIndicatorUpdater(
            self.start_date,
            self.end_date,
            self.drop_date,
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

        output_df = self.prepare_df(res0)
        filtered_output_df = updater.preprocess_output(output_df)

        td = TemporaryDirectory()

        create_export_csv(filtered_output_df, td.name,
                          start_date=self.start_date,
                          end_date=self.end_date,
                          geo_res=res0["geo_level"],
                          sensor=Config.signal_name)
        # check outputs
        expected_name = f"20200501_geography_{Config.signal_name}.csv"
        assert exists(join(td.name, expected_name))
        output_data = pd.read_csv(join(td.name, expected_name))
        assert (
                output_data.columns == ["geo_id", "val", "se", "sample_size"]
        ).all()
        assert (output_data.geo_id == ["a", "b"]).all()
        assert np.array_equal(output_data.val.values, np.array([0.1, 1]))

        # for privacy we do not usually report SEs
        assert np.isnan(output_data.se.values).all()
        assert np.isnan(output_data.sample_size.values).all()

        expected_name = f"20200502_geography_{Config.signal_name}.csv"
        assert exists(join(td.name, expected_name))
        output_data = pd.read_csv(join(td.name, expected_name))
        assert (
                output_data.columns == ["geo_id", "val", "se", "sample_size"]
        ).all()
        assert (output_data.geo_id == ["a"]).all()
        assert np.array_equal(output_data.val.values, np.array([0.5]))
        assert np.isnan(output_data.se.values).all()
        assert np.isnan(output_data.sample_size.values).all()

        expected_name = f"20200504_geography_{Config.signal_name}.csv"
        assert exists(join(td.name, expected_name))
        output_data = pd.read_csv(join(td.name, expected_name))
        assert (
                output_data.columns == ["geo_id", "val", "se", "sample_size"]
        ).all()
        assert (output_data.geo_id == ["a", "b"]).all()
        assert np.array_equal(output_data.val.values, np.array([1.5, 3]))
        assert np.isnan(output_data.se.values).all()
        assert np.isnan(output_data.sample_size.values).all()

        td.cleanup()

    def test_write_to_csv_with_se_results(self):
        obfuscated_name = PARAMS["indicator"]["obfuscated_prefix"]
        signal_name = obfuscated_name + "_" + Config.signal_weekday_name
        updater = ClaimsHospIndicatorUpdater(
            self.start_date,
            self.end_date,
            self.drop_date,
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

        output_df = self.prepare_df(res0)
        filtered_output_df = updater.preprocess_output(output_df)

        td = TemporaryDirectory()

        create_export_csv(filtered_output_df, td.name,
                          start_date=self.start_date,
                          end_date=self.end_date,
                          geo_res=res0["geo_level"],
                          sensor=signal_name)

        # check outputs
        expected_name = f"20200501_geography_{signal_name}.csv"
        assert exists(join(td.name, expected_name))
        output_data = pd.read_csv(join(td.name, expected_name))
        assert (
                output_data.columns == ["geo_id", "val", "se", "sample_size"]
        ).all()
        assert (output_data.geo_id == ["a", "b"]).all()
        assert np.array_equal(output_data.val.values, np.array([0.1, 1]))
        assert np.array_equal(output_data.se.values, np.array([0.1, 0.5]))
        assert np.isnan(output_data.sample_size.values).all()
        td.cleanup()

    def test_preprocess_wrong_results(self):
        updater = ClaimsHospIndicatorUpdater(
            self.start_date,
            self.end_date,
            self.drop_date,
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

        # nan value for included loc-date
        res1 = deepcopy(res0)
        res1["rates"]["a"][1] = np.nan
        output_df = self.prepare_df(res1)
        with pytest.raises(AssertionError):
            updater.preprocess_output(output_df)

        # nan se for included loc-date
        res2 = deepcopy(res0)
        res2["se"]["a"][1] = np.nan
        output_df = self.prepare_df(res2)
        with pytest.raises(AssertionError):
            updater.preprocess_output(output_df)

        # large se value
        res3 = deepcopy(res0)
        res3["se"]["a"][0] = 10
        output_df = self.prepare_df(res3)
        with pytest.raises(AssertionError):
            updater.preprocess_output(output_df)
