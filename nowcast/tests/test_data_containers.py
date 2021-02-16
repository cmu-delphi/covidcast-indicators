import pytest
import numpy as np
from datetime import date

from delphi_nowcast.data_containers import LocationSeries


class TestLocationSeries:

    def test_add_data(self):
        test_ls = LocationSeries(data={date(2020, 1, 1): 2})
        test_ls.add_data(date(2020, 1, 3), 4)
        assert test_ls == LocationSeries(data={date(2020, 1, 1): 2, date(2020, 1, 3): 4})

    def test_get_data_range_out_of_bounds(self):
        test_ls = LocationSeries(data={date(2020, 1, 1): 7, date(2020, 1, 2): 8, date(2020, 1, 3): 9})
        with pytest.raises(ValueError,
                           match="Data range must be within existing dates "
                                 "2020-01-01 to 2020-01-03"):
            test_ls.get_data_range(date(2019, 12, 31), date(2020, 1, 3))
        with pytest.raises(ValueError,
                           match="Data range must be within existing dates "
                                 "2020-01-01 to 2020-01-03"):
            test_ls.get_data_range(date(2020, 1, 1), date(2020, 1, 4))

    def test_get_data_range_no_impute(self):
        test_ls = LocationSeries(data={date(2020, 1, 1): 7, date(2020, 1, 2): np.nan, date(2020, 1, 3): 9})
        assert test_ls.get_data_range(date(2020, 1, 1), date(2020, 1, 3), None) == [7, np.nan, 9]
        assert test_ls.get_data_range(date(2020, 1, 1), date(2020, 1, 2), None) == [7, np.nan]

    def test_get_data_range_mean_impute(self):
        test_ls = LocationSeries(data={date(2020, 1, 1): 7, date(2020, 1, 2): np.nan, date(2020, 1, 3): 9})
        assert test_ls.get_data_range(date(2020, 1, 1), date(2020, 1, 3), "mean") == [7, 8.0, 9]
        assert test_ls.get_data_range(date(2020, 1, 1), date(2020, 1, 2), "mean") == [7, 7]

    def test_get_data_range_invalid_impute(self):
        test_ls = LocationSeries(data={date(2020, 1, 1): 7, date(2020, 1, 2): np.nan, date(2020, 1, 3): 9})
        with pytest.raises(ValueError, match="Invalid imputation method. Must be None or 'mean'"):
            test_ls.get_data_range(date(2020, 1, 1), date(2020, 1, 3), "fakeimpute")

    def test_no_data(self):
        test_ls = LocationSeries()
        with pytest.raises(ValueError, match="No data"):
            test_ls.dates
        with pytest.raises(ValueError, match="No data"):
            test_ls.values
