import pytest
import numpy as np
from datetime import date

from delphi_nowcast.data_containers import LocationSeries


class TestLocationSeries:

    def test___post_init__(self):
        with pytest.raises(ValueError, match="Length of dates and values differs.") as exc:
            LocationSeries(dates=[date(2020, 1, 1)], values=[1, 1])
        with pytest.raises(ValueError, match="Duplicate dates not allowed.") as exc:
            LocationSeries(dates=[date(2020, 1, 1), date(2020, 1, 1)], values=[1, 2])

    def test_empty(self):
        test_ls = LocationSeries(dates=[], values=[])
        assert test_ls.empty
        test_ls = LocationSeries(dates=[date(2020, 1, 1)], values=[2])
        assert not test_ls.empty

    def test_add_data(self):
        test_ls = LocationSeries(dates=[date(2020, 1, 1)], values=[2])
        test_ls.add_data(3, 4)
        assert test_ls == LocationSeries(dates=[date(2020, 1, 1), date(2020, 1, 3)], values=[2, 4])

    def test_get_data_range_out_of_bounds(self):
        test_ls = LocationSeries(dates=[date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3)],
                                 values=[7, 8, 9])
        with pytest.raises(ValueError) as exc:
            test_ls.get_data_range(date(2019, 12, 31), date(2020, 1, 3))
            assert str(exc.value) == "Data range must be within existing dates 1-3"
        with pytest.raises(ValueError) as exc:
            test_ls.get_data_range(date(2020, 1, 1), date(2020, 1, 4))
            assert str(exc.value) == "Data range must be within existing dates 1-3"

    def test_get_data_range_no_impute(self):
        test_ls = LocationSeries(dates=[date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3)],
                                 values=[7, np.nan, 9])
        assert test_ls.get_data_range(date(2020, 1, 1), date(2020, 1, 3), None) == [7, np.nan, 9]
        assert test_ls.get_data_range(date(2020, 1, 1), date(2020, 1, 2), None) == [7, np.nan]

    def test_get_data_range_mean_impute(self):
        test_ls = LocationSeries(dates=[date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3)],
                                 values=[7, np.nan, 9])
        assert test_ls.get_data_range(date(2020, 1, 1), date(2020, 1, 3), "mean") == [7, 8.0, 9]
        assert test_ls.get_data_range(date(2020, 1, 1), date(2020, 1, 2), "mean") == [7, 7]

    def test_get_data_range_invalid_impute(self):
        test_ls = LocationSeries(dates=[date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3)],
                                 values=[7, np.nan, 9])
        with pytest.raises(ValueError) as exc:
            test_ls.get_data_range(date(2020, 1, 1), date(2020, 1, 3), "fakeimpute")
            assert str(exc.value) == "Invalid imputation method. Must be None or 'mean'"
