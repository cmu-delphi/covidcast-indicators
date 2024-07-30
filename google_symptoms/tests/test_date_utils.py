from datetime import datetime, date, timedelta

import pandas as pd
from freezegun import freeze_time
from conftest import TEST_DIR, NEW_DATE

import covidcast

from delphi_utils.validator.utils import lag_converter
from delphi_google_symptoms.constants import FULL_BKFILL_START_DATE
from delphi_google_symptoms.date_utils import generate_query_dates, generate_num_export_days, generate_patch_dates


class TestDateUtils:

    @freeze_time("2021-01-05")
    def test_generate_query_dates(self):
        output = generate_query_dates(
            datetime.strptime("20201230", "%Y%m%d"),
            datetime.combine(date.today(), datetime.min.time()),
            14,
            False
        )

        expected = [datetime(2020, 12, 16),
                    datetime(2021, 1, 5)]
        assert set(output) == set(expected)

    @freeze_time("2021-01-05")
    def test_generate_query_dates_custom(self):
        output = generate_query_dates(
            datetime.strptime("20200201", "%Y%m%d"),
            datetime.combine(date.today(), datetime.min.time()),
            14,
            True
        )

        expected = [datetime(2020, 1, 26),
                    datetime(2021, 1, 5)]
        assert set(output) == set(expected)

    def test_generate_export_dates(self, params, logger, monkeypatch):
        metadata_df = pd.read_csv(f"{TEST_DIR}/test_data/covid_metadata.csv")
        monkeypatch.setattr(covidcast, "metadata", lambda: metadata_df)

        num_export_days = generate_num_export_days(params, logger)
        expected_num_export_days = params["indicator"]["num_export_days"]
        assert num_export_days == expected_num_export_days

    def test_generate_export_dates_normal(self, params_w_no_date, logger, monkeypatch):
        metadata_df = pd.read_csv(f"{TEST_DIR}/test_data/covid_metadata.csv")
        monkeypatch.setattr(covidcast, "metadata", lambda: metadata_df)

        num_export_days = generate_num_export_days(params_w_no_date, logger)

        max_expected_lag = lag_converter(params_w_no_date["validation"]["common"]["max_expected_lag"])
        global_max_expected_lag = max(list(max_expected_lag.values()))
        expected_num_export_days = params_w_no_date["validation"]["common"]["span_length"] + global_max_expected_lag

        assert num_export_days == expected_num_export_days

    def test_generate_export_date_missing(self, params_w_no_date, logger, monkeypatch):
        metadata_df = pd.read_csv(f"{TEST_DIR}/test_data/covid_metadata_missing.csv")
        monkeypatch.setattr(covidcast, "metadata", lambda: metadata_df)

        num_export_days = generate_num_export_days(params_w_no_date, logger)
        expected_num_export_days = (date.today() - FULL_BKFILL_START_DATE.date()).days + 1
        assert num_export_days == expected_num_export_days

    def generate_expected_start_end_dates(self, params_, issue_date):
        # Actual dates reported on issue dates June 27-29, 2024, by the old
        # version of the google-symptoms indicator
        # (https://github.com/cmu-delphi/covidcast-indicators/tree/b338a0962bf3a63f70a83f0b719516f914b098e2).
        # The patch module should be able to recreate these dates.
        dates_dict = {
            "2024-06-27": [ '2024-06-02', '2024-06-03', '2024-06-04', '2024-06-05', '2024-06-06', '2024-06-07', '2024-06-08', '2024-06-09', '2024-06-10', '2024-06-11', '2024-06-12', '2024-06-13', '2024-06-14', '2024-06-15', '2024-06-16', '2024-06-17', '2024-06-18', '2024-06-19', '2024-06-20', '2024-06-21', '2024-06-22'],
            "2024-06-28": ['2024-06-03', '2024-06-04', '2024-06-05', '2024-06-06', '2024-06-07', '2024-06-08', '2024-06-09', '2024-06-10', '2024-06-11', '2024-06-12', '2024-06-13', '2024-06-14', '2024-06-15', '2024-06-16', '2024-06-17', '2024-06-18', '2024-06-19', '2024-06-20', '2024-06-21', '2024-06-22', '2024-06-23'],
            "2024-06-29": ['2024-06-04', '2024-06-05', '2024-06-06','2024-06-07', '2024-06-08', '2024-06-09', '2024-06-10', '2024-06-11', '2024-06-12', '2024-06-13', '2024-06-14', '2024-06-15', '2024-06-16', '2024-06-17', '2024-06-18', '2024-06-19', '2024-06-20', '2024-06-21', '2024-06-22', '2024-06-23', '2024-06-24'],
        }

        dates_dict = {
            datetime.strptime(key, "%Y-%m-%d"): [
                datetime.strptime(listvalue, "%Y-%m-%d") for listvalue in value
            ] for key, value in dates_dict.items()
        }

        dates = dates_dict[issue_date]

        return {
            "export_start_date": min(dates[6:21]),
            "export_end_date": max(dates[6:21])
        }

    def test_generate_patch_dates(self, params_w_patch, logger):
        max_expected_lag = lag_converter(params_w_patch["validation"]["common"]["max_expected_lag"])
        global_max_expected_lag = max(list(max_expected_lag.values()))
        num_export_days = params_w_patch["validation"]["common"]["span_length"]

        issue_date = datetime.strptime(params_w_patch["patch"]["start_issue"], "%Y-%m-%d")
        end_issue = datetime.strptime(params_w_patch["patch"]["end_issue"], "%Y-%m-%d")

        patch_date_dict = generate_patch_dates(params_w_patch)

        while issue_date <= end_issue:
            # in the patch script the date generated by generate_patch_dates becomes the export_start_date and export_end_date
            patch_settings = patch_date_dict[issue_date]
            expected_dict = self.generate_expected_start_end_dates(params_w_patch, issue_date)
            expected_dict["num_export_days"] = num_export_days # unmodified

            assert patch_settings == expected_dict

            issue_date += timedelta(days=1)
