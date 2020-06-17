# standard
import pytest

# third party
from delphi_utils import read_params
import numpy as np
import numpy.random as nr
import pandas as pd

# first party
from delphi_emr_hosp.config import Config
from delphi_emr_hosp.load_data import load_combined_data
from delphi_emr_hosp.sensor import EMRHospSensor

CONFIG = Config()
PARAMS = read_params()
CLAIMS_FILEPATH = PARAMS["input_claims_file"]
EMR_FILEPATH = PARAMS["input_emr_file"]
DROP_DATE = pd.to_datetime(PARAMS["drop_date"])


class TestLoadData:
    fips_combined_data = load_combined_data(EMR_FILEPATH, CLAIMS_FILEPATH, DROP_DATE,
                                            "fips")
    hrr_combined_data = load_combined_data(EMR_FILEPATH, CLAIMS_FILEPATH, DROP_DATE,
                                           "hrr")

    def test_backfill(self):
        num0 = pd.Series([0, 1, 2, 3, 4, 5, 6, 7, 8], dtype=float)
        den0 = pd.Series([0, 10, 10, 10, 10, 10, 10, 100, 101], dtype=float)

        num1, den1 = EMRHospSensor.backfill(num0, den0, k=7, min_visits_to_fill=0)
        pd.testing.assert_series_equal(num0, num1)
        pd.testing.assert_series_equal(den0, den1)

        num2, den2 = EMRHospSensor.backfill(num0, den0, k=7, min_visits_to_fill=11)
        exp_num2 = pd.Series([0, 1, 3, 5, 7, 9, 11, 7, 8], dtype=float)
        exp_den2 = pd.Series([0, 10, 20, 20, 20, 20, 20, 100, 101], dtype=float)
        pd.testing.assert_series_equal(exp_num2, num2)
        pd.testing.assert_series_equal(exp_den2, den2)

        num3, den3 = EMRHospSensor.backfill(num0, den0, k=7, min_visits_to_fill=100)
        exp_num3 = pd.Series([0, 1, 3, 6, 10, 15, 21, 7, 8], dtype=float)
        exp_den3 = pd.Series([0, 10, 20, 30, 40, 50, 60, 100, 101], dtype=float)
        pd.testing.assert_series_equal(exp_num3, num3)
        pd.testing.assert_series_equal(exp_den3, den3)

        num4, den4 = EMRHospSensor.backfill(num0, den0, k=3, min_visits_to_fill=100)
        exp_num4 = pd.Series([0, 1, 3, 6, 10, 14, 18, 7, 8], dtype=float)
        exp_den4 = pd.Series([0, 10, 20, 30, 40, 40, 40, 100, 101], dtype=float)
        pd.testing.assert_series_equal(exp_num4, num4)
        pd.testing.assert_series_equal(exp_den4, den4)

    def test_fit_fips(self):
        date_range = pd.date_range("2020-05-01", "2020-05-20")
        all_fips = self.fips_combined_data.index.get_level_values('fips').unique()
        sample_fips = nr.choice(all_fips, 10)

        for fips in sample_fips:
            sub_data = self.fips_combined_data.loc[fips]
            sub_data = sub_data.reindex(date_range, fill_value=0)
            res0 = EMRHospSensor.fit(sub_data, date_range[0], fips)
            # first value is burn-in
            assert np.min(res0["rate"][1:]) > 0
            assert np.max(res0["rate"][1:]) <= 100

            if np.all(np.isnan(res0["se"])):
                assert res0["incl"].sum() == 0
            else:
                # binomial standard error, hence largest possible value is
                # 100 * (0.5 / sqrt(MIN_DEN))
                assert np.nanmax(res0["se"]) <= 100 * (0.5 / np.sqrt(Config.MIN_DEN))
                assert np.nanmin(res0["se"]) > 0
                assert res0["incl"].sum() > 0

    def test_fit_hrrs(self):
        date_range = pd.date_range("2020-05-01", "2020-05-20")
        all_hrrs = self.hrr_combined_data.index.get_level_values('hrr').unique()
        sample_hrrs = nr.choice(all_hrrs, 10)

        for hrr in sample_hrrs:
            sub_data = self.hrr_combined_data.loc[hrr]
            sub_data = sub_data.reindex(date_range, fill_value=0)
            res0 = EMRHospSensor.fit(sub_data, date_range[0], hrr)
            # first value is burn-in
            assert np.min(res0["rate"][1:]) > 0
            assert np.max(res0["rate"][1:]) <= 100

            if np.all(np.isnan(res0["se"])):
                assert res0["incl"].sum() == 0
            else:
                # binomial standard error, hence largest possible value is
                # 100 * (0.5 / sqrt(MIN_DEN))
                assert np.nanmax(res0["se"]) <= 100 * (0.5 / np.sqrt(Config.MIN_DEN))
                assert np.nanmin(res0["se"]) > 0
                assert res0["incl"].sum() > 0
