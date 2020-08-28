from datetime import date

import numpy as np
import pandas as pd
import pytest

from delphi_quidel_covidtest import data_tools


class TestDataTools:

    @pytest.mark.parametrize("p,n,expected", [
        # 0 cases
        (0, 8, 0),
        (1, 6, 0),
        # nonzero cases
        (0.5, 2, 0.125),
        (0.4, 6, 0.04),
    ])
    def test__prop_var(self, p, n, expected):
        assert data_tools._prop_var(p, n) == expected
        # test div/0 case
        with pytest.raises(ZeroDivisionError):
            data_tools._prop_var(0.1, 0)

    @pytest.mark.parametrize("input_df, first, last, expected", [
        # do nothing case
        (pd.DataFrame([1., 2], index=[date(2020, 1, 2), date(2020, 1, 3)]),
         date(2020, 1, 2),
         date(2020, 1, 3),
         pd.DataFrame([1., 2], index=[date(2020, 1, 2), date(2020, 1, 3)])),
        # add to end case
        (pd.DataFrame([1, 2], index=[date(2020, 1, 2), date(2020, 1, 3)]),
         date(2020, 1, 2),
         date(2020, 1, 4),
         pd.DataFrame([1., 2., 0.], index=[date(2020, 1, 2), date(2020, 1, 3), date(2020, 1, 4)])),
        # add to start case
        (pd.DataFrame([1, 2], index=[date(2020, 1, 2), date(2020, 1, 4)]),
         date(2020, 1, 2),
         date(2020, 1, 4),
         pd.DataFrame([1, 0, 2], index=[date(2020, 1, 2), date(2020, 1, 3), date(2020, 1, 4)])),
        # fill middle case
        (pd.DataFrame([1, 2], index=[date(2020, 1, 2), date(2020, 1, 3)]),
         date(2020, 1, 1),
         date(2020, 1, 3),
         pd.DataFrame([0., 1., 2.], index=[date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3)])),
        # add both sides case
        (pd.DataFrame([1.], index=[date(2020, 1, 2)]),
         date(2020, 1, 1),
         date(2020, 1, 3),
         pd.DataFrame([0., 1., 0.], index=[date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3)])),
        # nan case + fill both sides
        (pd.DataFrame([np.nan], index=[date(2020, 1, 2)]),
         date(2020, 1, 1),
         date(2020, 1, 3),
         pd.DataFrame([0., 0., 0.], index=[date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3)]))

    ])
    def test_fill_dates(self, input_df, first, last, expected):
        assert data_tools.fill_dates(input_df, first, last).equals(expected)

    @pytest.mark.parametrize("k, expected", [
        # check all k from 1 to n+1
        (1, np.array([1, 2, 3, 4])),
        (2, np.array([1, 3, 5, 7])),
        (3, np.array([1, 3, 6, 9])),
        (4, np.array([1, 3, 6, 10])),
        (5, np.array([1, 3, 6, 10])),
    ])
    def test__slide_window_sum(self, k, expected):
        arr = np.array([1, 2, 3, 4])
        assert np.array_equal(data_tools._slide_window_sum(arr, k), expected)
        # non int case
        with pytest.raises(ValueError):
            data_tools._slide_window_sum(np.array([1]), 'abc')

    @pytest.mark.parametrize("min_obs, expected", [
        (1, np.array([0, 0, 0, 0])),
        (2, np.array([1/2, 0, 0, 0])),
        (8, np.array([1, 1, 5/6, 4/8])),
    ])
    def test__geographical_pooling(self, min_obs, expected):
        tpooled_tests = np.array([1, 2, 3, 4])
        tpooled_ptests = np.array([2, 4, 6, 8])
        assert np.array_equal(
            data_tools._geographical_pooling(tpooled_tests, tpooled_ptests, min_obs),
            expected)
        # nan case
        with pytest.raises(ValueError):
            data_tools._geographical_pooling(np.array([np.nan]), np.array([1]), 1)

    @pytest.mark.parametrize("min_obs, expected_pos_prop, expected_se, expected_sample_sz", [
        (3,  # one case of tests < min_obs
         np.array([np.nan, 1/2, 1/2, 4/10]),
         np.array([np.nan, np.sqrt(0.25/4), np.sqrt(0.25/6), np.sqrt(0.24/10)]),
         np.array([np.nan, 4, 6, 10])),
        (1,  # no cases of tests < min_obs
         np.array([1/2, 2/4, 3/6, 4/10]),
         np.array([np.sqrt(0.25/2), np.sqrt(0.25/4), np.sqrt(0.25/6), np.sqrt(0.24/10)]),
         np.array([2, 4, 6, 10])),
    ])
    def test_raw_positive_prop(self, min_obs, expected_pos_prop, expected_se, expected_sample_sz):
        positives = np.array([1, 2, 3, 4])
        tests = np.array([2, 4, 6, 10])
        output = data_tools.raw_positive_prop(positives, tests, min_obs)
        # np.array_equal() doesn't compare nan's
        assert np.allclose(output[0], expected_pos_prop, equal_nan=True)
        assert np.allclose(output[1], expected_se, equal_nan=True)
        assert np.allclose(output[2], expected_sample_sz, equal_nan=True)
        # nan case
        with pytest.raises(ValueError):
            data_tools.raw_positive_prop(np.array([np.nan]), np.array([1]), 3)
        # positives > tests case
        with pytest.raises(ValueError):
            data_tools.raw_positive_prop(np.array([3]), np.array([1]), 3)
        # min obs <= 0 case
        with pytest.raises(ValueError):
            data_tools.raw_positive_prop(np.array([1]), np.array([1]), 0)

    @pytest.mark.parametrize("min_obs, pool_days, parent_positives, parent_tests,"
                             "expected_prop, expected_se, expected_sample_sz", [
        (3,  # no parents case
         2,
         None,
         None,
         np.array([np.nan, 1/2, 1/2, 7/16]),
         np.array([np.nan, np.sqrt(0.25/6), np.sqrt(0.25/10), np.sqrt(63/256/16)]),
         np.array([np.nan, 6, 10, 16]),
         ),
        (3,  # parents case
         2,
         np.array([3, 7, 9, 11]),
         np.array([5, 10, 15, 20]),
         np.array([1.6/3, 1/2, 1/2, 7/16]),
         np.array([np.sqrt(56/225/3), np.sqrt(0.25/6), np.sqrt(0.25/10), np.sqrt(63/256/16)]),
         np.array([3, 6, 10, 16]),
         ),
    ])
    def test_smoothed_positive_prop(self, min_obs, pool_days, parent_positives,
                                    parent_tests, expected_prop, expected_se, expected_sample_sz):
        positives = np.array([1, 2, 3, 4])
        tests = np.array([2, 4, 6, 10])
        output = data_tools.smoothed_positive_prop(positives, tests, min_obs, pool_days,
                                                   parent_positives, parent_tests)
        assert np.allclose(output[0], expected_prop, equal_nan=True)
        assert np.allclose(output[1], expected_se, equal_nan=True)
        assert np.allclose(output[2], expected_sample_sz, equal_nan=True)

        # nan case
        with pytest.raises(ValueError):
            data_tools.smoothed_positive_prop(np.array([np.nan]), np.array([1]), 1, 1)
        # positives > tests case
        with pytest.raises(ValueError):
            data_tools.smoothed_positive_prop(np.array([2]), np.array([1]), 1, 1)
        # nan case with parent
        with pytest.raises(ValueError):
            data_tools.smoothed_positive_prop(np.array([1]), np.array([1]), 1, 1,
                                              np.array([np.nan]), np.array([np.nan]))
        # positives > tests case with parent
        with pytest.raises(ValueError):
            data_tools.smoothed_positive_prop(np.array([1]), np.array([1]), 1, 1,
                                              np.array([3]), np.array([1]))
        # min obs <= 0 case
        with pytest.raises(ValueError):
            data_tools.smoothed_positive_prop(np.array([1]), np.array([1]), 0, 1)
        # pool_days <= 0 case
        with pytest.raises(ValueError):
            data_tools.smoothed_positive_prop(np.array([1]), np.array([1]), 1, 0)
        # pool_days non int case
        with pytest.raises(ValueError):
            data_tools.smoothed_positive_prop(np.array([1]), np.array([1]), 1, 1.5)

    @pytest.mark.parametrize("min_obs, expected_tests_per_device, expected_sample_sz", [
        (3,  # one case of tests < min_obs
         np.array([np.nan, 2, 1/2, 10/4]),
         np.array([np.nan, 4, 3, 10])),
        (1,  # no cases of tests < min_obs
         np.array([2, 2, 1/2, 10/4]),
         np.array([2, 4, 3, 10])),
    ])
    def test_raw_tests_per_device(self, min_obs, expected_tests_per_device, expected_sample_sz):
        devices = np.array([1, 2, 6, 4])
        tests = np.array([2, 4, 3, 10])
        output = data_tools.raw_tests_per_device(devices, tests, min_obs)
        assert np.allclose(output[0], expected_tests_per_device, equal_nan=True)
        assert np.allclose(output[1], np.repeat(np.nan, len(devices)), equal_nan=True)
        assert np.allclose(output[2], expected_sample_sz, equal_nan=True)
        # nan case
        with pytest.raises(ValueError):
            data_tools.raw_tests_per_device(np.array([np.nan]), np.array([1]), 3)
        # min obs <= 0 case
        with pytest.raises(ValueError):
            data_tools.raw_tests_per_device(np.array([1]), np.array([1]), 0)

    @pytest.mark.parametrize("min_obs, pool_days, parent_devices, parent_tests,"
                             "expected_prop, expected_se, expected_sample_sz", [
        (3,  # no parents case
         2,
         None,
         None,
         np.array([np.nan, 2, 5/6, 8/7]),
         np.repeat(np.nan, 4),
         np.array([np.nan, 6, 10, 16]),
         ),
        (3,  # no parents case
         2,
         np.array([3, 7, 25, 11]),
         np.array([5, 10, 15, 20]),
         np.array([3/1.6, 2, 5/6, 8/7]),
         np.repeat(np.nan, 4),
         np.array([3, 6, 10, 16]),
         ),
    ])
    def test_smoothed_tests_per_device(self, min_obs, pool_days, parent_devices, parent_tests,
                                       expected_prop, expected_se, expected_sample_sz):
        devices = np.array([1, 2, 10, 4])
        tests = np.array([2, 4, 6, 10])
        output = data_tools.smoothed_tests_per_device(devices, tests, min_obs, pool_days,
                                                      parent_devices, parent_tests)
        assert np.allclose(output[0], expected_prop, equal_nan=True)
        assert np.allclose(output[1], expected_se, equal_nan=True)
        assert np.allclose(output[2], expected_sample_sz, equal_nan=True)

        # nan case
        with pytest.raises(ValueError):
            data_tools.smoothed_tests_per_device(np.array([np.nan]), np.array([1]), 1, 1)
        # nan case with parent
        with pytest.raises(ValueError):
            data_tools.smoothed_tests_per_device(np.array([1]), np.array([1]), 1, 1,
                                                 np.array([np.nan]), np.array([np.nan]))
        # min obs <= 0 case
        with pytest.raises(ValueError):
            data_tools.smoothed_tests_per_device(np.array([1]), np.array([1]), 0, 1)
        # pool_days <= 0 case
        with pytest.raises(ValueError):
            data_tools.smoothed_tests_per_device(np.array([1]), np.array([1]), 1, 0)
        # pool_days non int case
        with pytest.raises(ValueError):
            data_tools.smoothed_tests_per_device(np.array([1]), np.array([1]), 1, 1.5)