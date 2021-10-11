"""Tests for running the CDC Vaccine indicator."""
from itertools import product
from os import listdir
from os.path import join
from unittest.mock import patch

import pandas as pd

from delphi_cdc_vaccines.run import run_module

def local_fetch(url, cache):
    return pd.read_csv(url)

class TestRun:
    """Tests for the `run_module()` function."""
    PARAMS = {
        "common": {
            "export_dir": "./receiving",
            "input_dir": "./input_cache"
        },
        "indicator": {
            "base_url": "./test_data/small.csv",
            "export_start_date": "2021-08-10"
        }
    }

    def test_output_files_exist(self):
        """Test that the expected output files exist."""
        run_module(self.PARAMS)

        csv_files = [f for f in listdir("receiving") if f.endswith(".csv")]

        dates = [
            "20210810",
            "20210811",
            "20210812",
            "20210813",
            "20210814",
            "20210815",
            "20210816",
            "20210817",
            "20210818",
            "20210819",
        ]
        geos = ["state", "hrr", "hhs", "nation", "msa"]

        expected_files = []
        for metric in ["cumulative_counts_tot_vaccine",
                                    "incidence_counts_tot_vaccine",
                                    "cumulative_counts_tot_vaccine_12P",
                                    "incidence_counts_tot_vaccine_12P",
                                    "cumulative_counts_tot_vaccine_18P",
                                    "incidence_counts_tot_vaccine_18P",
                                    "cumulative_counts_tot_vaccine_65P",
                                    "incidence_counts_tot_vaccine_65P",
                                    "cumulative_counts_part_vaccine",
                                    "incidence_counts_part_vaccine",
                                    "cumulative_counts_part_vaccine_12P",
                                    "incidence_counts_part_vaccine_12P",
                                    "cumulative_counts_part_vaccine_18P",
                                    "incidence_counts_part_vaccine_18P",
                                    "cumulative_counts_part_vaccine_65P",
                                    "incidence_counts_part_vaccine_65P"]: 
            for date in dates:
                for geo in geos:
                    expected_files += [date + "_" + geo + "_" + metric + ".csv"]
                    if not("cumulative" in metric) and not (date in dates[:6]):
                        expected_files += [date + "_" + geo + "_" + metric + "_7dav.csv"]

        assert set(csv_files) == set(expected_files)

    def test_output_file_format(self):
        """Test that the output files have the proper format."""
        run_module(self.PARAMS)

        df = pd.read_csv(
            join("receiving", "20210819_state_cumulative_counts_tot_vaccine.csv")
        )
        assert (df.columns.values == ["geo_id", "val", "missing_se", "missing_sample_size"]).all()