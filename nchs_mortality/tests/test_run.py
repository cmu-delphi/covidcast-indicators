import datetime as dt
from os import listdir
from os.path import join
import pytest

import pandas as pd


class TestRun:
    # the 14th was a Monday
    @pytest.mark.parametrize("date", ["2020-09-14", "2020-09-17", "2020-09-18"])
    def test_output_files_exist(self, run_as_module, date):
        is_mon_or_thurs = dt.datetime.strptime(date, "%Y-%m-%d").weekday() == (0 or 3)

        folders = ["daily_cache"]
        if is_mon_or_thurs:
            folders.append("receiving")

        for output_folder in folders:
            csv_files = listdir(output_folder)

            geos = ["nation", "state"]
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
            for geo in geos:
                for d in dates:
                    for metric in metrics:
                        if metric == "deaths_percent_of_expected":
                            expected_files += [f"weekly_{d}_{geo}_{metric}.csv"]
                        else:
                            for sensor in sensors:
                                expected_files += [f"weekly_{d}_{geo}_{metric}_{sensor}.csv"]
            assert set(expected_files).issubset(set(csv_files))

    # the 14th was a Monday
    @pytest.mark.parametrize("date", ["2020-09-14", "2020-09-17", "2020-09-18"])
    def test_output_file_format(self, run_as_module, date):
        is_mon_or_thurs = dt.datetime.strptime(date, "%Y-%m-%d").weekday() == (0 or 3)

        folders = ["daily_cache"]
        if is_mon_or_thurs:
            folders.append("receiving")

        geos = ["nation", "state"]
        for geo in geos:
            for output_folder in folders:
                df = pd.read_csv(
                    join(output_folder, f"weekly_202026_{geo}_deaths_covid_incidence_prop.csv")
                )
                expected_columns = [
                    "geo_id", "val", "se", "sample_size",
                    "missing_val", "missing_se", "missing_sample_size"
                ]
                assert (df.columns.values == expected_columns).all()

    # the 14th was a Monday
    @pytest.mark.parametrize("date", ["2020-09-14", "2020-09-17", "2020-09-18"])
    def test_national_prop(self, run_as_module, date):
        is_mon_or_thurs = dt.datetime.strptime(date, "%Y-%m-%d").weekday() == (0 or 3)

        folders = ["daily_cache"]
        if is_mon_or_thurs:
            folders.append("receiving")

        for output_folder in folders:
            num = pd.read_csv(
                join(output_folder, f"weekly_202026_nation_deaths_covid_incidence_num.csv")
            )
            prop = pd.read_csv(
                join(output_folder, f"weekly_202026_nation_deaths_covid_incidence_prop.csv")
            )
            # hardcoded value from the geomapper CSV
            US_POPULATION = 334103109
            INCIDENCE_BASE = 100000
            numeric_value = num.iloc[0]["val"] / US_POPULATION * INCIDENCE_BASE
            assert(numeric_value > 0)
            assert(prop.iloc[0]["val"] == round(numeric_value, 7))
