from datetime import datetime

import pandas as pd

from delphi_quidel.pull import (
    fix_zipcode,
    fix_date,
    pull_quidel_data,
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
    def test_pull_quidel_data(self):

        dfs, _ = pull_quidel_data({
            "static_file_dir": "../static",
            "input_cache_dir": "./cache",
            "export_start_date": {"covid_ag": "2020-06-30", "flu_ag": "2020-05-30"},
            "export_end_date": {"covid_ag": "2020-07-09", "flu_ag": "2020-07-05"},
            "pull_start_date": {"covid_ag": "2020-07-09","flu_ag": "2020-07-05"},
            "pull_end_date": {"covid_ag": "", "flu_ag": "2020-07-10"},
            "mail_server": "imap.exchange.andrew.cmu.edu",
            "account": "delphi-datadrop@andrew.cmu.edu",
            "password": "",
            "sender": "",
            "wip_signal": [""],
            "test_mode": True
        })

        # For covid_ag
        df = dfs["covid_ag"]
        first_date = df["timestamp"].min().date()
        last_date = df["timestamp"].max().date()

        assert [first_date.month, first_date.day] == [7, 2]
        assert [last_date.month, last_date.day] == [7, 23]
        assert (df.columns ==\
            ['timestamp', 'zip', 'totalTest', 'numUniqueDevices', 'positiveTest']).all()

        # For covid_ag
        df = dfs["flu_ag"]
        first_date = df["timestamp"].min().date()
        last_date = df["timestamp"].max().date()

        assert [first_date.month, first_date.day] == [6, 22]
        assert [last_date.month, last_date.day] == [8, 17]
        assert (df.columns ==\
            ['timestamp', 'zip', 'totalTest', 'numUniqueDevices', 'positiveTest']).all()


    def test_check_intermediate_file(self):

        previous_dfs, pull_start_dates = check_intermediate_file("./cache/test_cache_with_file",
                                                                 {"covid_ag": None, "flu_ag":None})
        assert previous_dfs["covid_ag"] is not None
        assert previous_dfs["flu_ag"] is not None
        assert pull_start_dates["covid_ag"] is not None
        assert pull_start_dates["flu_ag"] is not None

        previous_dfs, pull_start_dates = check_intermediate_file("./cache/test_cache_without_file",
                                                                 {"covid_ag": None, "flu_ag":None})
        assert previous_dfs["covid_ag"] is None
        assert previous_dfs["flu_ag"] is None
        assert pull_start_dates["covid_ag"] is None
        assert pull_start_dates["flu_ag"] is None

    def test_check_export_end_date(self):

        _end_date = datetime(2020, 7, 7)
        test_dates = ["", "2020-07-07", "2020-06-15"]
        tested = []
        for test_date in test_dates:
            export_end_dates = {"covid_ag": test_date, "flu_ag": ""}
            tested.append(check_export_end_date(export_end_dates, _end_date,
                                                END_FROM_TODAY_MINUS)["covid_ag"])
        expected = [datetime(2020, 7, 2), datetime(2020, 7, 2), datetime(2020, 6,15)]

        assert tested == expected

    def test_check_export_start_date(self):

        test_dates = ["", "2020-06-20", "2020-04-20"]
        tested = []
        for test_date in test_dates:
            export_start_dates = {"covid_ag": test_date, "flu_ag": ""}
            export_end_dates = {"covid_ag": datetime(2020, 7, 2), "flu_ag": datetime(2020, 7, 2)}
            tested.append(check_export_start_date(export_start_dates,
                                                  export_end_dates,
                                                  EXPORT_DAY_RANGE)["covid_ag"])
        expected = [datetime(2020, 5, 26), datetime(2020, 6, 20), datetime(2020, 5, 26)]

        assert tested == expected
