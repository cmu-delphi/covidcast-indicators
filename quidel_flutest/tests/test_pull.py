from delphi_utils import read_params

from datetime import datetime, date

import numpy as np
import pandas as pd

from delphi_quidel_flutest.pull import (
    fix_zipcode,
    fix_date,
    pull_quidel_flutest,
    check_intermediate_file
)


class TestFixData:
    def test_fix_zipcode(self):

        df = pd.DataFrame({"ZipCode":[2837,  29570, "15213-0436", "02134-3611"]})
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
    def test_pull_quidel_flutest(self):
        
        params = read_params()
        mail_server = params["mail_server"]
        account = params["account"]
        password = params["password"]
        sender = params["sender"]
        
        pull_start_date = date(2020, 6, 10)
        pull_end_date = date(2020, 6, 12)
        
        df, _ = pull_quidel_flutest(pull_start_date, pull_end_date, mail_server,
                               account, sender, password) 
        
        first_date = df["timestamp"].min().date() 
        last_date = df["timestamp"].max().date() 
        
        assert [first_date.month, first_date.day] == [3, 11]
        assert [last_date.month, last_date.day] == [6, 11]
        assert (df.columns== ['timestamp', 'zip', 'totalTest', 'numUniqueDevices', 'positiveTest']).all()
        

    def test_check_intermediate_file(self):
        
        previous_df, pull_start_date = check_intermediate_file("./cache/test_cache_with_file", None)
        assert previous_df is not None
        assert pull_start_date is not None
        # Put the test file back
        previous_df.to_csv("./cache/test_cache_with_file/pulled_until_20200710.csv", index=False)

        previous_df, pull_start_date = check_intermediate_file("./cache/test_cache_without_file", None)
        assert previous_df is None
        assert pull_start_date is None