from datetime import datetime, date
import json
from unittest.mock import patch
import tempfile
import os

from delphi_hhs.run import _date_to_int, int_date_to_previous_day_datetime, generate_date_ranges, \
    make_signal, make_geo, run_module, pop_proportion
from delphi_hhs.constants import CONFIRMED, SUM_CONF_SUSP, SMOOTHERS, GEOS, SIGNALS, CONFIRMED_PROP, SUM_CONF_SUSP_PROP
from delphi_utils.geomap import GeoMapper
from freezegun import freeze_time
import numpy as np
import pandas as pd
import pytest


def test__date_to_int():
    """Check that dates are converted to the right int."""
    assert _date_to_int(date(2020, 5, 1)) == 20200501


def test_date_conversion():
    """Check that we convert dates properly between Epidata and datetime format."""
    data = pd.DataFrame({"date": [20200101, 20201231]})
    result = int_date_to_previous_day_datetime(data.date)
    expected_result = [
        datetime(year=2019, month=12, day=31),
        datetime(year=2020, month=12, day=30)
    ]
    for got, expected in zip(result, expected_result):
        assert isinstance(got, datetime), f"Bad type: {type(got)}\n{result}"
        assert got == expected


def test_generate_date_ranges():
    """Check ranges generated partition the specified inputs."""
    assert generate_date_ranges(date(2020, 1, 1), date(2020, 1, 1)) == \
           [{'from': 20200101, 'to': 20200101}]
    assert generate_date_ranges(date(2020, 1, 1), date(2020, 1, 31)) == \
           [{'from': 20200101, 'to': 20200131}]
    assert generate_date_ranges(date(2020, 1, 1), date(2020, 2, 1)) == \
           [{'from': 20200101, 'to': 20200131},
            {'from': 20200201, 'to': 20200201}]
    assert generate_date_ranges(date(2020, 1, 1), date(2020, 5, 12)) == \
           [{'from': 20200101, 'to': 20200131},
            {'from': 20200201, 'to': 20200302},
            {'from': 20200303, 'to': 20200402},
            {'from': 20200403, 'to': 20200503},
            {'from': 20200504, 'to': 20200512}]


def test_make_signal():
    """Check that constructed signals sum the correct columns."""
    data = pd.DataFrame({
        'state': ['NA'],
        'date': [20200102],
        'previous_day_admission_adult_covid_confirmed': [1],
        'previous_day_admission_adult_covid_suspected': [2],
        'previous_day_admission_pediatric_covid_confirmed': [4],
        'previous_day_admission_pediatric_covid_suspected': [8]
    })

    expected_confirmed = pd.DataFrame({
        'state': ['na'],
        'timestamp': [datetime(year=2020, month=1, day=1)],
        'val': [5.],
    })
    pd.testing.assert_frame_equal(expected_confirmed, make_signal(data, CONFIRMED))
    pd.testing.assert_frame_equal(expected_confirmed, make_signal(data, CONFIRMED_PROP))

    expected_sum = pd.DataFrame({
        'state': ['na'],
        'timestamp': [datetime(year=2020, month=1, day=1)],
        'val': [15.],
    })
    pd.testing.assert_frame_equal(expected_sum, make_signal(data, SUM_CONF_SUSP))
    pd.testing.assert_frame_equal(expected_sum, make_signal(data, SUM_CONF_SUSP_PROP))

    with pytest.raises(Exception):
        make_signal(data, "zig")

def test_pop_proportion():
    geo_mapper = GeoMapper()
    test_df = pd.DataFrame({  
        'state': ['PA'],
        'state_code': [42],
        'timestamp': [datetime(year=2020, month=1, day=1)],
        'val': [15.],})
    pd.testing.assert_frame_equal(
        pop_proportion(test_df, geo_mapper),
        pd.DataFrame({
            'state': ['PA'],
            'state_code': [42],
            'timestamp': [datetime(year=2020, month=1, day=1)],
            'val': [0.1171693],})
    )

    test_df= pd.DataFrame({  
        'state': ['WV'],
        'state_code': [54],
        'timestamp': [datetime(year=2020, month=1, day=1)],
        'val': [150.],})

    pd.testing.assert_frame_equal(
        pop_proportion(test_df, geo_mapper),
        pd.DataFrame({
            'state': ['WV'],
            'state_code': [54],
            'timestamp': [datetime(year=2020, month=1, day=1)],
            'val': [8.3698491],})
    )

def test_make_geo():
    """Check that geographies transform correctly."""
    test_timestamp = datetime(year=2020, month=1, day=1)
    geo_mapper = GeoMapper()

    data = pd.DataFrame({
        'state': ['PA', 'WV', 'OH'],
        'state_code': [42, 54, 39],
        'timestamp': [test_timestamp] * 3,
        'val': [1., 2., 4.],
    })

    template = {
        'se': np.nan,
        'sample_size': np.nan,
    }
    expecteds = {
        "state": pd.DataFrame(
            dict(template,
                 geo_id=data.state,
                 timestamp=data.timestamp,
                 val=data.val)),
        "hhs": pd.DataFrame(
            dict(template,
                 geo_id=['3', '5'],
                 timestamp=[test_timestamp] * 2,
                 val=[3., 4.])),
        "nation": pd.DataFrame(
            dict(template,
                 geo_id=['us'],
                 timestamp=[test_timestamp],
                 val=[7.]))
    }
    for geo, expected in expecteds.items():
        result = make_geo(data, geo, geo_mapper)
        for series in ["geo_id", "timestamp", "val", "se", "sample_size"]:
            pd.testing.assert_series_equal(expected[series], result[series], obj=f"{geo}:{series}")


@freeze_time("2020-01-01")
@patch("delphi_epidata.Epidata.covid_hosp")
def test_output_files(mock_covid_hosp):
    with open("test_response.json", "r") as f:
        test_response = json.load(f)
    mock_covid_hosp.return_value = test_response
    with tempfile.TemporaryDirectory() as tmpdir:
        params = {
            "common": {
                "export_dir": tmpdir
            }
        }
        run_module(params)
        # 9 days in test data, so should be 9 days of unsmoothed and 3 days for smoothed
        expected_num_files = len(GEOS) * len(SIGNALS) * 9 + len(GEOS) * len(SIGNALS) * 3
        assert len(os.listdir(tmpdir)) == expected_num_files


@freeze_time("2020-02-03")
@patch("delphi_hhs.run.create_export_csv")
@patch("delphi_epidata.Epidata.covid_hosp")
def test_ignore_last_range_no_results(mock_covid_hosp, mock_export):
    mock_covid_hosp.side_effect = [
        {"result": 1,
         "epidata":
             {"state": ["placeholder"],
              "date": ["20200101"],
              "previous_day_admission_adult_covid_confirmed": [0],
              "previous_day_admission_adult_covid_suspected": [0],
              "previous_day_admission_pediatric_covid_confirmed": [0],
              "previous_day_admission_pediatric_covid_suspected": [0]
              }
         },
        {"result": -2, "message": "no results"}
    ]
    mock_export.return_value = None
    params = {
        "common": {
            "export_dir": "./receiving"
        }
    }
    assert not run_module(params)  # function should not raise value error and has no return value
