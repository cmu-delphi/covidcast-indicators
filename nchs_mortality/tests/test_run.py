import datetime as dt
from os import listdir
from os.path import join
import pytest

import pandas as pd


class TestRun:
    @pytest.mark.parametrize("date", ["2020-09-14", "2020-09-18"])
    def test_output_files_exist(self, run_as_module, date):
        is_monday = dt.datetime.strptime(date, "%Y-%m-%d").weekday() == 0

        folders = ["daily_cache"]
        if is_monday:
            folders.append("receiving")

        for output_folder in folders:
            csv_files = listdir(output_folder)

            dates = [
                "202030",
                "202031",
                "202032",
                "202033",
                "202034",
                "202035",
                "202036",
            ]
            metrics = ['deaths_covid_incidence',
                       'deaths_allcause_incidence',
                       'deaths_percent_of_expected',
                       'deaths_pneumonia_notflu_incidence',
                       'deaths_covid_and_pneumonia_notflu_incidence',
                       'deaths_flu_incidence',
                       'deaths_pneumonia_or_flu_or_covid_incidence']
            sensors = ["num", "prop"]

            expected_files = []
            for d in dates:
                for metric in metrics:
                    if metric == "deaths_percent_of_expected":
                        expected_files += ["weekly_" + d + "_state_" \
                                           + metric + ".csv"]
                    else:
                        for sensor in sensors:
                            expected_files += ["weekly_" + d + "_state_" \
                                               + metric + "_" + sensor + ".csv"]
            assert set(expected_files).issubset(set(csv_files))

    @pytest.mark.parametrize("date", ["2020-09-14", "2020-09-18"])
    def test_output_file_format(self, run_as_module, date):
        is_monday = dt.datetime.strptime(date, "%Y-%m-%d").weekday() == 0

        folders = ["daily_cache"]
        if is_monday:
            folders.append("receiving")

        for output_folder in folders:
            df = pd.read_csv(
                join(output_folder, "weekly_202026_state_deaths_covid_incidence_prop.csv")
            )
            expected_columns = [
                "geo_id", "val", "se", "sample_size",
                "missing_val", "missing_se", "missing_sample_size"
            ]
            assert (df.columns.values == expected_columns).all()
