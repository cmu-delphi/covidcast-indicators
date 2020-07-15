import pytest

from delphi_utils import read_params

from os.path import join, exists
from tempfile import TemporaryDirectory
from datetime import datetime

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from delphi_quidel_covidtest.pull import (
    get_from_email,
    fix_zipcode,
    fix_date,
    pull_quidel_covidtest
)


class TestFixData:
    def test_fix_zipcode(self):

        df = pd.DataFrame({"Zip":[2837,  29570, "15213-0436", "02134-3611"]})
        df = fix_zipcode(df)

        assert set(df["zip"]) == set([2837, 29570, 15213, 2134])

    def test_fix_date(self):

        df = pd.DataFrame({"StorageDate":[datetime(2020, 5, 19), datetime(2020, 6, 9),
                                          datetime(2020, 6, 14), datetime(2020, 7, 10)],
                           "TestDate":[datetime(2020, 1, 19), datetime(2020, 6, 10),
                                          datetime(2020, 6, 11), datetime(2020, 7, 2)]})
        df = fix_date(df)

        assert set(df["timestamp"]) == set([datetime(2020, 5, 19), 
                                            datetime(2020, 6, 11), datetime(2020, 7, 2)])

class TestingPullData:
    def test_pull_quidel_covidtest(self):

        return

    
