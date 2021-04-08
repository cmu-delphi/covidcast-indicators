from os import listdir
from os.path import join
from itertools import product

import pandas as pd


class TestRun:
    def test_output_files_exist(self, run_as_module):
        csv_files = set(listdir("receiving"))

        dates = [d.strftime("%Y%m%d") for d in pd.date_range("20200726", "20200811")]
        geos = ["county", "state", "hhs", "msa", "hrr", "nation"]
        metrics = ["anosmia", "ageusia", "sum_anosmia_ageusia"]
        smoother = ["raw", "smoothed"]

        expected_files = {
            f"{date}_{geo}_{metric}_{smoother}_search.csv"
            for date, geo, metric, smoother in product(dates, geos, metrics, smoother)
        }

        assert csv_files == expected_files

        df = pd.read_csv(
            join("receiving", "20200810_state_anosmia_smoothed_search.csv")
        )
        expected_columns = [
            "geo_id", "val", "se", "sample_size",
            "missing_val", "missing_se", "missing_sample_size"
        ]
        assert (df.columns.values == expected_columns).all()
