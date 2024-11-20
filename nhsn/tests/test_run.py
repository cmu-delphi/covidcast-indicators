import os
from pathlib import Path

import pandas as pd
from epiweeks import Week

from delphi_nhsn.constants import SIGNALS_MAP, PRELIM_SIGNALS_MAP


class TestRun:
    # the 14th was a Monday
    def generate_week_file_prefix(self, dates):

        epiweeks_lst = [ Week.fromdate(pd.to_datetime(str(date))) for date in dates ]
        date_prefix = [
            str(t.year) + str(t.week).zfill(2)
            for t in epiweeks_lst
        ]
        return date_prefix

    def test_output_files_exist(self, params, run_as_module):
        export_dir = params["common"]["export_dir"]
        csv_files = [f.name for f in Path(export_dir).glob("*.csv")]
        geos = ["nation", "state"]
        metrics = list(SIGNALS_MAP.keys()) + list(PRELIM_SIGNALS_MAP.keys())
        dates = [
            "2021-08-21", "2021-08-28", "2021-09-04",
            "2021-09-11", "2021-09-18", "2021-09-25",
            "2021-10-02", "2021-10-16"
        ]
        date_prefix = self.generate_week_file_prefix(dates)

        expected_files = []
        for geo in geos:
            for d in date_prefix:
                for metric in metrics:
                    expected_files += [f"weekly_{d}_{geo}_{metric}.csv"]

        assert set(expected_files).issubset(set(csv_files))

        for geo in geos:
            df = pd.read_csv(
                f"{export_dir}/weekly_{date_prefix[3]}_{geo}_{metrics[0]}.csv")

            expected_columns = [
                "geo_id", "val", "se", "sample_size",
            ]
            assert (df.columns.values == expected_columns).all()

        for file in Path(export_dir).glob("*.csv"):
            os.remove(file)
