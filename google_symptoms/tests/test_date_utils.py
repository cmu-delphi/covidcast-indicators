from datetime import datetime, date

import pandas as pd
from freezegun import freeze_time
from conftest import TEST_DIR, NEW_DATE

from delphi_utils.validator.utils import lag_converter
from delphi_google_symptoms.constants import FULL_BKFILL_START_DATE
from delphi_google_symptoms.date_utils import generate_query_dates, generate_export_dates, generate_patch_dates
class TestDateUtils:

    @freeze_time("2021-01-05")
    def test_generate_query_dates(self):
        output = generate_query_dates(
            datetime.strptime("20201230", "%Y%m%d"),
            datetime.combine(date.today(), datetime.min.time()),
            14,
            False
        )

        expected = [datetime(2020, 12, 24),
                    datetime(2021, 1, 5)]
        assert set(output) == set(expected)

    @freeze_time("2021-01-05")
    def test_generate_query_dates(self):
        output = generate_query_dates(
            datetime.strptime("20200201", "%Y%m%d"),
            datetime.combine(date.today(), datetime.min.time()),
            14,
            False
        )

        expected = [datetime(2020, 12, 16),
                    datetime(2021, 1, 5)]
        assert set(output) == set(expected)

    def test_generate_export_dates_normal(self, params, logger, monkeypatch):
        import covidcast
        metadata_df = pd.read_csv(f"{TEST_DIR}/test_data/covid_metadata.csv")
        monkeypatch.setattr(covidcast, "metadata", lambda: metadata_df)
        start_date, end_date, num_export_days = generate_export_dates(params, logger)

        max_expected_lag = lag_converter(params["validation"]["common"].get("max_expected_lag", {"all": 4}))
        global_max_expected_lag = max(list(max_expected_lag.values()))
        expected_num_export_days = params["validation"]["common"].get("span_length", 14) + global_max_expected_lag

        assert start_date == FULL_BKFILL_START_DATE
        assert end_date == date.today()
        assert num_export_days == expected_num_export_days

    def test_generate_export_date_start_changed(self, params_diff_start, logger, monkeypatch):
        import covidcast
        metadata_df = pd.read_csv(f"{TEST_DIR}/test_data/covid_metadata.csv")
        monkeypatch.setattr(covidcast, "metadata", lambda: metadata_df)
        start_date, end_date, num_export_days = generate_export_dates(params_diff_start, logger)

        max_expected_lag = lag_converter(params_diff_start["validation"]["common"].get("max_expected_lag", {"all": 4}))
        global_max_expected_lag = max(list(max_expected_lag.values()))
        expected_num_export_days = params_diff_start["validation"]["common"].get("span_length", 14) + global_max_expected_lag

        assert start_date == datetime.strptime(NEW_DATE, "%Y-%m-%d")
        assert end_date == date.today()
        assert num_export_days == expected_num_export_days


    def test_generate_export_dates_end_changed(self, params_diff_end, logger, monkeypatch):
        import covidcast
        metadata_df = pd.read_csv(f"{TEST_DIR}/test_data/covid_metadata.csv")
        monkeypatch.setattr(covidcast, "metadata", lambda: metadata_df)
        start_date, end_date, num_export_days = generate_export_dates(params_diff_end, logger)

        max_expected_lag = lag_converter(params_diff_end["validation"]["common"].get("max_expected_lag", {"all": 4}))
        global_max_expected_lag = max(list(max_expected_lag.values()))
        expected_num_export_days = params_diff_end["validation"]["common"].get("span_length", 14) + global_max_expected_lag

        assert start_date == FULL_BKFILL_START_DATE
        assert end_date == datetime.strptime(NEW_DATE, "%Y-%m-%d")
        assert num_export_days == expected_num_export_days

    def test_generate_patch_dates(self, params_w_patch, logger, monkeypatch):
        import covidcast
        metadata_df = pd.read_csv(f"{TEST_DIR}/test_data/covid_metadata_missing.csv")
        monkeypatch.setattr(covidcast, "metadata", lambda: metadata_df)
        max_expected_lag = lag_converter(params_w_patch["validation"]["common"].get("max_expected_lag", {"all": 4}))
        global_max_expected_lag = max(list(max_expected_lag.values()))
        expected_num_export_days = params_w_patch["validation"]["common"].get("span_length", 14) + global_max_expected_lag

        output = generate_patch_dates(params_w_patch)

        for date_info in output:
            issue_date, daterange = list(*date_info.items())
            num_export_dates = (daterange[1] - daterange[0]).days
            # plus one to expected to account for inclusive counting
            assert num_export_dates == expected_num_export_days + 1

