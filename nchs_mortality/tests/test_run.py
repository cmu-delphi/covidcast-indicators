import pytest

import datetime as dt
from os import listdir
from os.path import join

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
                "202016",
                "202017",
                "202018",
                "202019",
                "202020",
                "202021",
                "202022",
                "202023",
                "202024",
                "202025",
                "202026",
            ]
            metrics = [
                    'covid_deaths', 'total_deaths', 'pneumonia_deaths',
                    'pneumonia_and_covid_deaths', 'influenza_deaths',
                    'pneumonia_influenza_or_covid_19_deaths'
            ]
            sensors = ["num", "prop"]

            expected_files = []
            for date in dates:
                for metric in metrics:
                    if metric == "percent_of_expected_deaths":
                        expected_files += ["weekly_" + date + "_state_wip_" \
                                           + metric + ".csv"]
                    else:
                        for sensor in sensors:
                            expected_files += ["weekly_" + date + "_state_wip_" \
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
                join(output_folder, "weekly_202026_state_wip_covid_deaths_prop.csv")
            )
            assert (df.columns.values == ["geo_id", "val", "se", "sample_size"]).all()
