# standard

import numpy as np
import numpy.random as nr
import pandas as pd
# first party
from delphi_changehc.config import Config
from delphi_changehc.load_data import load_combined_data
from delphi_changehc.sensor import CHCSensor

CONFIG = Config()
PARAMS = {
    "indicator": {
        "input_denom_file": "test_data/20200601_Counts_Products_Denom.dat.gz",
        "input_covid_file": "test_data/20200601_Counts_Products_Covid.dat.gz",
        "drop_date": "2020-06-01"
    }
}
COVID_FILEPATH = PARAMS["indicator"]["input_covid_file"]
DENOM_FILEPATH = PARAMS["indicator"]["input_denom_file"]
DROP_DATE = pd.to_datetime(PARAMS["indicator"]["drop_date"])

class TestLoadData:
    combined_data = load_combined_data(DENOM_FILEPATH, COVID_FILEPATH, DROP_DATE,
                                       "fips")

    def test_backfill(self):
        num0 = np.array([0, 1, 2, 3, 4, 5, 6, 7, 8], dtype=float).reshape(-1, 1)
        den0 = np.array([0, 10, 10, 10, 10, 10, 10, 100, 101], dtype=float)

        num1, den1 = CHCSensor.backfill(num0, den0, k=7, min_visits_to_fill=0)
        assert np.array_equal(num0, num1)
        assert np.array_equal(den0, den1)

        num2, den2 = CHCSensor.backfill(num0, den0, k=7, min_visits_to_fill=11)
        exp_num2 = np.array([0, 1, 3, 5, 7, 9, 11, 7, 8], dtype=float).reshape(-1, 1)
        exp_den2 = np.array([0, 10, 20, 20, 20, 20, 20, 100, 101], dtype=float)
        assert np.array_equal(exp_num2, num2)
        assert np.array_equal(exp_den2, den2)
        #
        num3, den3 = CHCSensor.backfill(num0, den0, k=7, min_visits_to_fill=100)
        exp_num3 = np.array([0, 1, 3, 6, 10, 15, 21, 7, 8], dtype=float).reshape(-1, 1)
        exp_den3 = np.array([0, 10, 20, 30, 40, 50, 60, 100, 101], dtype=float)
        assert np.array_equal(exp_num3, num3)
        assert np.array_equal(exp_den3, den3)
        #
        num4, den4 = CHCSensor.backfill(num0, den0, k=3, min_visits_to_fill=100)
        exp_num4 = np.array([0, 1, 3, 6, 10, 14, 18, 7, 8], dtype=float).reshape(-1, 1)
        exp_den4 = np.array([0, 10, 20, 30, 40, 40, 40, 100, 101], dtype=float)
        assert np.array_equal(exp_num4, num4)
        assert np.array_equal(exp_den4, den4)

    def test_fit_fips(self):
        date_range = pd.date_range("2020-05-01", "2020-05-20")
        all_fips = self.combined_data.index.get_level_values('fips').unique()
        sample_fips = nr.choice(all_fips, 10)

        for fips in sample_fips:
            sub_data = self.combined_data.loc[fips]
            sub_data = sub_data.reindex(date_range, fill_value=0)
            res0 = CHCSensor.fit(sub_data, date_range[0], fips)
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
