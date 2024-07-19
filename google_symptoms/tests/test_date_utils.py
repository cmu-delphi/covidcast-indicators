from datetime import datetime, date

import pandas as pd
import pytest
from freezegun import freeze_time
from conftest import TEST_DIR

from delphi_utils.validator.utils import lag_converter
from delphi_google_symptoms.date_utils import generate_date_range, _generate_candidate_dates, generate_patch_dates
class TestDateUtils:
    @freeze_time("2021-01-05")
    def test_get_date_range_recent_export_start_date(self):
        output = generate_date_range(
            datetime.strptime("20201230", "%Y%m%d"),
            datetime.combine(date.today(), datetime.min.time()),
            14
        )

        expected = [datetime(2020, 12, 24),
                    datetime(2021, 1, 5)]
        assert set(output) == set(expected)

    @freeze_time("2021-01-05")
    def test_get_date_range(self):
        output = generate_date_range(
            datetime.strptime("20200201", "%Y%m%d"),
            datetime.combine(date.today(), datetime.min.time()),
            14
        )

        expected = [datetime(2020, 12, 16),
                    datetime(2021, 1, 5)]
        assert set(output) == set(expected)

    def test_generate_candidate_dates_normal(self, params, logger, monkeypatch):
        import covidcast
        metadata_df = pd.read_csv(f"{TEST_DIR}/test_data/covid_metadata.csv")
        monkeypatch.setattr(covidcast, "metadata", lambda: metadata_df)
        output = _generate_candidate_dates(params, logger)
        print("hi")

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
            assert num_export_dates == expected_num_export_days

