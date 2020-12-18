from datetime import datetime, date

from delphi_hhs.run import _date_to_int, int_date_to_previous_day_datetime, generate_date_ranges, \
    make_signal
from delphi_hhs.constants import CONFIRMED, SUM_CONF_SUSP
import pandas as pd
import pytest

def test__date_to_int():
    """Check that dates are converted to the right int."""
    assert _date_to_int(date(2020, 5, 1)) == 20200501

def test_date_conversion():
    """Check that we convert dates properly between Epidata and datetime format."""
    data = pd.DataFrame({ "date":[20200101, 20201231] })
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
        'previous_day_admission_adult_covid_confirmed':[1],
        'previous_day_admission_adult_covid_suspected':[2],
        'previous_day_admission_pediatric_covid_confirmed':[4],
        'previous_day_admission_pediatric_covid_suspected':[8]
    })

    expected_confirmed = pd.DataFrame({
        'geo_id': ['na'],
        'timestamp': [datetime(year=2020, month=1, day=1)],
        'val': [5],
        'se': None,
        'sample_size': None
    })
    pd.testing.assert_frame_equal(expected_confirmed, make_signal(data, CONFIRMED))

    expected_sum = pd.DataFrame({
        'geo_id': ['na'],
        'timestamp': [datetime(year=2020, month=1, day=1)],
        'val': [15],
        'se': None,
        'sample_size': None
    })
    pd.testing.assert_frame_equal(expected_sum, make_signal(data, SUM_CONF_SUSP))

    with pytest.raises(Exception):
        make_signal(data, "zig")
