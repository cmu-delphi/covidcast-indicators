"""Tests for datafetcher.py."""

from datetime import date
import mock
import numpy as np
import pandas as pd
from delphi_validator.datafetcher import (FILENAME_REGEX,
                                          make_date_filter,
                                          get_geo_signal_combos,
                                          threaded_api_calls)
from delphi_validator.errors import ValidationFailure


class TestDataFetcher:
    """Tests for various data fetching utilities."""
    def test_make_date_filter(self):
        date_filter = make_date_filter(date(2020, 4, 4), date(2020, 5, 23))

        assert date_filter(FILENAME_REGEX.match("20200420_a_b.csv"))
        assert not date_filter(FILENAME_REGEX.match("20200403_a_b.csv"))
        assert not date_filter(FILENAME_REGEX.match("20200620_a_b.csv"))
        assert not date_filter(FILENAME_REGEX.match("202006_a_b.csv"))

    @mock.patch("covidcast.metadata")
    def test_get_geo_signal_combos(self, mock_metadata):
        """Test that the geo signal combos are correctly pulled from the covidcast metadata."""
        mock_metadata.return_value = pd.DataFrame({"data_source": ["a", "a", "a",
                                                                   "b", "b", "b"],
                                                   "signal": ["w", "x", "x",
                                                              "y", "y", "z"],
                                                   "geo_type": ["state", "state", "county",
                                                                "hrr", "msa", "msa"]
                                                  })

        assert set(get_geo_signal_combos("a")) == set([("state", "w"),
                                                       ("state", "x"),
                                                       ("county", "x")])
        assert set(get_geo_signal_combos("b")) == set([("hrr", "y"),
                                                       ("msa", "y"),
                                                       ("msa", "z")])

    @mock.patch("covidcast.signal")
    def test_threaded_api_calls(self, mock_signal):
        """Test that calls to the covidcast API are made."""

        signal_data_1 = pd.DataFrame({"geo_value": ["1044"],
                                      "stderr": [None],
                                      "value": [3],
                                      "issue": [10],
                                      "lag": [7],
                                      "sample_size": [None],
                                      "time_value": [10]
                                     })
        signal_data_2 = pd.DataFrame({"geo_value": ["0888"],
                                      "stderr": [2],
                                      "value": [14],
                                      "issue": [10],
                                      "lag": [1],
                                      "sample_size": [100],
                                      "time_value": [8]
                                     })

        def mock_signal_return_fn(unused_data_source, signal_type, unused_start_date,
                                  unused_end_date, geo_type):
            """Function to return data when covidcast.signal() is called."""
            if signal_type == "a":
                return signal_data_1
            if geo_type == "county":
                return signal_data_2
            return None

        mock_signal.side_effect = mock_signal_return_fn

        processed_signal_data_1 = pd.DataFrame({"geo_id": ["1044"],
                                                "val": [3],
                                                "se": [np.nan],
                                                "sample_size": [np.nan],
                                                "time_value": [10]
                                               })
        processed_signal_data_2 = pd.DataFrame({"geo_id": ["0888"],
                                                "val": [14],
                                                "se": [2],
                                                "sample_size": [100],
                                                "time_value": [8]
                                               })
        expected = {
            ("county", "a"): processed_signal_data_1,
            ("county", "b"): processed_signal_data_2,
            ("state", "a"): processed_signal_data_1,
            ("state", "b"): ValidationFailure("api_data_fetch_error", "state b",
                                             "Error fetching data from 2020-03-10 "
                                             "to 2020-06-10 for data source: "
                                             "source, signal type: b, geo type: state")
        }
        actual = threaded_api_calls("source", date(2020, 3, 10), date(2020, 6, 10), expected.keys())

        assert set(expected.keys()) == set(actual.keys())
        for k, v in actual.items():
            if isinstance(v, pd.DataFrame):
                pd.testing.assert_frame_equal(v, expected[k])
            else:
                assert str(v) == str(expected[k])
