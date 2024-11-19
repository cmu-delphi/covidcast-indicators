from os import listdir

import pandas as pd
import pytest

from delphi_nhsn.constants import SIGNALS_MAP


class TestRun:
    # the 14th was a Monday
    def test_output_files_exist(self, params, run_as_module):
        for output_folder in params["common"]["export_dir"]:
            csv_files = listdir(output_folder)

            geos = ["nation", "state"]
            dates = [
                "2021-08-21", "2021-08-28", "2021-09-04",
                "2021-09-11", "2021-09-18", "2021-09-25",
                "2021-10-02", "2021-10-16"
            ]
            metrics = SIGNALS_MAP.values()

            expected_files = []
            for geo in geos:
                for d in dates:
                    for metric in metrics:
                        expected_files += [f"weekly_{d}_{geo}_{metric}.csv"]
            assert set(expected_files).issubset(set(csv_files))

    # def test_output_file_format(self, run_as_module, params):
    #     geos = ["nation", "state"]
    #     for geo in geos:
    #         for output_folder in params["common"]["export_dir"]:
    #             df = pd.read_csv(
    #                 join(output_folder, f"weekly_202026_{geo}_deaths_covid_incidence_prop.csv")
    #             )
    #             expected_columns = [
    #                 "geo_id", "val", "se", "sample_size",
    #                 "missing_val", "missing_se", "missing_sample_size"
    #             ]
    #             assert (df.columns.values == expected_columns).all()
