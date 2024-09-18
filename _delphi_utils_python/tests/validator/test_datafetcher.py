"""Tests for datafetcher.py."""

from datetime import date, datetime
import mock
import json
from pathlib import Path
import numpy as np
import pandas as pd
import pytest
from requests.exceptions import HTTPError
import requests_mock
from delphi_epidata import delphi_epidata
from delphi_utils.validator.datafetcher import (FILENAME_REGEX,
                                                make_date_filter,
                                                get_geo_signal_combos,
                                                threaded_api_calls)
from delphi_utils.validator.errors import ValidationFailure


TEST_DIR = Path(__file__).parent.parent

class TestDataFetcher:
    """Tests for various data fetching utilities."""
    def test_make_date_filter(self):
        date_filter = make_date_filter(date(2020, 4, 4), date(2020, 5, 23))

        assert date_filter(FILENAME_REGEX.match("20200420_a_b.csv"))
        assert not date_filter(FILENAME_REGEX.match("20200403_a_b.csv"))
        assert not date_filter(FILENAME_REGEX.match("20200620_a_b.csv"))
        assert not date_filter(FILENAME_REGEX.match("202006_a_b.csv"))

    # Solution from https://stackoverflow.com/questions/15753390/
    #how-can-i-mock-requests-and-the-response
    def mocked_requests_get(*args, **kwargs):
        # TODO #1863: convert to requests_mock
        class MockResponse:
            def __init__(self, json_data, status_code):
                self.json_data = json_data
                self.status_code = status_code

            def json(self):
                return self.json_data

            def raise_for_status(self):
                if self.status_code != 200:
                    raise HTTPError()
        if len(kwargs) == 0 or list(kwargs.keys())==["auth"]:
            return MockResponse([{'source': 'chng', 'db_source': 'chng'},
                {'source': 'covid-act-now', 'db_source': 'covid-act-now'}], 200)
        elif "params" in kwargs and kwargs["params"] == {'signal': 'chng:inactive'}:
            return MockResponse([{"signals": [{"active": False}]}], 200)
        elif args[0] == 'https://api.delphi.cmu.edu/epidata/covidcast_meta/' and \
                'delphi_epidata' in kwargs["headers"]["user-agent"]:
            with open(f"{TEST_DIR}/test_data/sample_epidata_metadata.json") as f:
                epidata = json.load(f)
                response = {"epidata": epidata, "result": 1, "message": "success"}
                return MockResponse(response, 200)
        elif args[0] == 'https://api.delphi.cmu.edu/epidata/covidcast/' and \
            'delphi_epidata' in kwargs["headers"]["user-agent"]:
            signal_type = args[1].get("signals")
            geo_type = args[1].get("geo_type")
            if signal_type == "a":
                with open(f"{TEST_DIR}/test_data/sample_epidata_signal_a.json") as f:
                    epidata = json.load(f)
                    response = {"epidata": epidata, "result": 1, "message": "success"}
                    return MockResponse(response, 200)
            if geo_type == "county":
                with open(f"{TEST_DIR}/test_data/sample_epidata_signal_county.json") as f:
                    epidata = json.load(f)
                    response = {"epidata": epidata, "result": 1, "message": "success"}
                    return MockResponse(response, 200)
            if geo_type == "state" and signal_type == "b":
                return MockResponse({"epidata": {}, "result": 0, "message": "failed"}, 200)
            return MockResponse({"epidata": {}, "result": 1, "message": "success"}, 200)
        else:
            return MockResponse([{"signals": [{"active": True}]}], 200)

    # the `kw` approach is needed here because otherwise pytest thinks the 
    # requests_mock arg is supposed to be a fixture
    @requests_mock.Mocker(kw="mock_requests")
    def test_bad_api_key(self, **kwargs):
        kwargs["mock_requests"].get("https://api.covidcast.cmu.edu/epidata/covidcast/meta", status_code=429)
        with pytest.raises(HTTPError):
            get_geo_signal_combos("chng", api_key="")

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_get_geo_signal_combos(self, mock_get):

        """Test that the geo signal combos are correctly pulled from the covidcast metadata."""
        assert set(get_geo_signal_combos("chng", api_key="")) == set(
            [("state", "smoothed_outpatient_cli"),
             ("state", "smoothed_outpatient_covid"),
             ("county", "smoothed_outpatient_covid")])
        assert set(get_geo_signal_combos("covid-act-now", api_key="")) == set(
            [("hrr", "pcr_specimen_positivity_rate"),
             ("msa", "pcr_specimen_positivity_rate"),
             ("msa", "pcr_specimen_total_tests")])

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_threaded_api_calls(self, mock_get):
        """Test that calls to the covidcast API are made."""
        processed_signal_data_1 = pd.DataFrame({"geo_id": ["1044"],
                                                "val": [3],
                                                "se": [np.nan],
                                                "sample_size": [np.nan],
                                                "time_value": [datetime.strptime("20200101", "%Y%m%d")],
                                               })
        processed_signal_data_2 = pd.DataFrame({"geo_id": ["0888"],
                                                "val": [14],
                                                "se": [2],
                                                "sample_size": [100],
                                                "time_value": [datetime.strptime("20200101", "%Y%m%d")],
                                               })
        expected = {
            ("county", "a"): processed_signal_data_1,
            ("county", "b"): processed_signal_data_2,
            ("state", "a"): processed_signal_data_1,
            ("state", "b"): ValidationFailure("api_data_fetch_error",
                                              geo_type="state",
                                              signal="b",
                                             message="Error: no API data was returned when "
                                             "fetching reference data from 2020-03-10 "
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
