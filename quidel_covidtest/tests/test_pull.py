from delphi_utils import read_params

from datetime import datetime, date

import numpy as np
import pandas as pd

from delphi_quidel_covidtest.pull import (
    fix_zipcode,
    fix_date,
    pull_quidel_covidtest,
    check_intermediate_file,
    check_export_end_date,
    check_export_start_date
)

END_FROM_TODAY_MINUS = 5
EXPORT_DAY_RANGE = 40

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
        
        df, _ = pull_quidel_covidtest(params) 
        
        first_date = df["timestamp"].min().date() 
        last_date = df["timestamp"].max().date() 
        
        assert [first_date.month, first_date.day] == [7, 2]
        assert [last_date.month, last_date.day] == [7, 23]
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
    
    def test_check_export_end_date(self):
        
        _end_date = datetime(2020, 7, 7)
        export_end_dates = ["", "2020-07-07", "2020-06-15"]
        tested = []
        for export_end_date in export_end_dates:
            tested.append(check_export_end_date(export_end_date, _end_date,
                                                END_FROM_TODAY_MINUS))
        expected = [datetime(2020, 7, 2), datetime(2020, 7, 2), datetime(2020, 6,15)]
        
        assert tested == expected
            
    def test_check_export_start_date(self):
        
        export_end_date = datetime(2020, 7, 2)
        export_start_dates = ["", "2020-06-20", "2020-04-20"]
        tested = []
        for export_start_date in export_start_dates:
            tested.append(check_export_start_date(export_start_date,
                                                  export_end_date, EXPORT_DAY_RANGE))
        expected = [datetime(2020, 5, 26), datetime(2020, 6, 20), datetime(2020, 5, 26)]
        
        assert tested == expected