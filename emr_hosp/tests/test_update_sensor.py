# standard
from copy import deepcopy
from os.path import join, exists
import pytest
from tempfile import TemporaryDirectory

# third party
import pandas as pd
import numpy as np

# first party
from delphi_emr_hosp.update_sensor import write_to_csv, update_sensor


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
        write_to_csv(res0, "name_of_signal", td.name)

        # check outputs
        expected_name = "20200502_geography_name_of_signal.csv"
        assert exists(join(td.name, expected_name))
        output_data = pd.read_csv(join(td.name, expected_name))
        assert (
                output_data.columns == ["geo_id", "val", "se", "direction", "sample_size"]
        ).all()
        assert (output_data.geo_id == ["a", "b"]).all()
        assert np.array_equal(output_data.val.values, np.array([0.1, 1]))

        # for privacy we do not report SEs
        # assert np.array_equal(output_data.se.values, np.array([0.1, 0.5]))
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
            write_to_csv(res1, "name_of_signal", td.name)

        # nan se for included loc-date
        res2 = deepcopy(res0)
        res2["se"]["a"][1] = np.nan
        with pytest.raises(AssertionError):
            write_to_csv(res2, "name_of_signal", td.name)

        # large sensor value
        res3 = deepcopy(res0)
        res3["rates"]["a"][0] = 95
        with pytest.raises(AssertionError):
            write_to_csv(res3, "name_of_signal", td.name)

        # large se value
        res4 = deepcopy(res0)
        res4["se"]["a"][0] = 10
        with pytest.raises(AssertionError):
            write_to_csv(res4, "name_of_signal", td.name)

        td.cleanup()
