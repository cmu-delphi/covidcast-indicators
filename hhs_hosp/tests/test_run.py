from datetime import datetime

import delphi_hhs
from delphi_hhs.run import int_date_to_previous_day_datetime
import pandas as pd

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
