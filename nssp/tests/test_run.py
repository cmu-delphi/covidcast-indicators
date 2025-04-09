import glob
import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd
from delphi_nssp.constants import GEOS, SIGNALS_MAP
from delphi_nssp.run import add_needed_columns
from epiweeks import Week


def remove_backup_and_receiving(params):
    export_dir = params["common"]["export_dir"]
    for file in Path(export_dir).glob("*.csv"):
        os.remove(file)

    today = pd.Timestamp.today().strftime("%Y%m%d")
    backup_dir = glob.glob(f"{Path(params['common']['backup_dir'])}/{today}*")
    for file in backup_dir:
        os.remove(file)

class TestRun:
    def test_add_needed_columns(self):
        df = pd.DataFrame({"geo_id": ["us"], "val": [1]})
        df = add_needed_columns(df, col_names=None)
        assert df.columns.tolist() == [
            "geo_id",
            "val",
            "se",
            "sample_size",
            "missing_val",
            "missing_se",
            "missing_sample_size",
        ]
        assert df["se"].isnull().all()
        assert df["sample_size"].isnull().all()

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

        metrics = list(SIGNALS_MAP.values())
        dates = ["2022-10-01", "2022-10-08", "2022-10-15"]
        date_prefix = self.generate_week_file_prefix(dates)

        expected_files = []
        for geo in GEOS:
            for d in date_prefix:
                for metric in metrics:
                    expected_files += [f"weekly_{d}_{geo}_{metric}.csv"]

        assert set(csv_files) == set(expected_files)

        # Verify that all generated file have correct columns.
        for geo in GEOS:
            df = pd.read_csv(
                f"{export_dir}/weekly_{date_prefix[2]}_{geo}_{metrics[0]}.csv")

            expected_columns = [
                "geo_id", "val", "se", "sample_size",
            ]
            assert set(expected_columns).issubset(set(df.columns.values))

            # Verify that there's no NA/empty values in the val columns
            assert not df["val"].isnull().any()

        remove_backup_and_receiving(params)

    def test_valid_hrr(self, run_as_module_hrr, params):
        export_dir = params["common"]["export_dir"]
        csv_files = [f for f in Path(export_dir).glob("*.csv")]

        # If summed normally, will give a huge number, If summed weighted, will give 100%
        for f in csv_files:
            df = pd.read_csv(f)
            assert (df.val == 100).all()

        remove_backup_and_receiving(params)

    def test_empty_data(self, run_as_module_empty, params, caplog):
        """
        Tests correct handling when there is a geo and signal combination that has no data.
        """

        caplog.set_level(logging.WARNING)
        run_as_module_empty()
        assert "No data for signal and geo combination" in caplog.text

        export_dir = params["common"]["export_dir"]
        csv_files = [f for f in Path(export_dir).glob("*.csv")]

        # Since only one national entry in page_no_data.json with numeric data,
        # while the two counties have no numeric fields, 
        # there should be no county, hrr, hhs, or msa files.
        assert not any(geo in f.name for geo in ["county", "hrr", "hhs", "msa"] for f in csv_files)

        remove_backup_and_receiving(params)
