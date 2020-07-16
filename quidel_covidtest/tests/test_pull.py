from delphi_utils import read_params

from datetime import datetime, date

import numpy as np
import pandas as pd

from delphi_quidel_covidtest.pull import (
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
        
        params = read_params()
        mail_server = params["mail_server"]
        account = params["account"]
        password = params["password"]
        sender = params["sender"]
        
        pull_start_date = date(2020, 6, 10)
        pull_end_date = date(2020, 6, 12)
        
        df = pull_quidel_covidtest(pull_start_date, pull_end_date, mail_server,
                               account, sender, password) 
        
        first_date = df["timestamp"].min().date() 
        last_date = df["timestamp"].max().date() 
        
        assert [first_date.month, first_date.day] == [5, 20]
        assert [last_date.month, last_date.day] == [6, 11]
        assert set(df.columns) == set(["timestamp", "zip", "totalTest", "positiveTest"])
        
        return

    
