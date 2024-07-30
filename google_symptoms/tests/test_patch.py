from datetime import datetime, timedelta
import unittest

import pandas as pd
from mock import patch as mock_patch
from pathlib import Path
import re
import shutil
from typing import List, Tuple

from delphi_google_symptoms.patch import patch
from delphi_utils.validator.utils import lag_converter

from delphi_google_symptoms.constants import SMOOTHERS_MAP, FULL_BKFILL_START_DATE
from delphi_google_symptoms.date_utils import generate_query_dates

from conftest import state_data_gap, covidcast_metadata, TEST_DIR


class TestPatchModule:

    def parse_csv_file(self, file_list: List[str]) -> Tuple[List[datetime]]:
        smoothed_list = list(set([datetime.strptime(f.name.split('_')[0],"%Y%m%d") for f in file_list if "smoothed" in f.name]))
        raw_list = list(set([datetime.strptime(f.name.split('_')[0],"%Y%m%d") for f in file_list if "raw" in f.name]))
        return sorted(smoothed_list), sorted(raw_list)

    def generate_expected_dates(self, params_, smoother, issue_date):
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

        if smoother == "raw":
            return dates
        else:
            return dates[6:21]

    def mocked_patch(self, params_):
        with mock_patch("delphi_google_symptoms.patch.read_params", return_value=params_), \
             mock_patch("delphi_google_symptoms.pull.pandas_gbq.read_gbq") as mock_read_gbq, \
             mock_patch("delphi_google_symptoms.pull.initialize_credentials", return_value=None), \
             mock_patch("delphi_google_symptoms.date_utils.covidcast.metadata", return_value=covidcast_metadata), \
             mock_patch("delphi_google_symptoms.run.GEO_RESOLUTIONS", new=["state"]):
            def side_effect(*args, **kwargs):
                if "symptom_search_sub_region_1_daily" in args[0]:
                    df = state_data_gap
                    pattern = re.compile(r'\d{4}-\d{2}-\d{2}')
                    start_date, end_date = re.findall(pattern, args[0])
                    return df[(df["date"] >= start_date) & (df["date"] <= end_date)]
                else:
                    return pd.DataFrame()

            mock_read_gbq.side_effect = side_effect
            start_date = datetime.strptime(params_["patch"]["start_issue"], "%Y-%m-%d")

            patch(params_)

            patch_path = Path(f"{TEST_DIR}/{params_['patch']['patch_dir']}")

            for issue_dir in sorted(list(patch_path.iterdir())):
                assert f'issue_{datetime.strftime(start_date, "%Y%m%d")}' == issue_dir.name

                smoothed_dates, raw_dates = self.parse_csv_file(list(Path(issue_dir, "google-symptom").glob("*.csv")))
                expected_smoothed_dates = self.generate_expected_dates(params_, "smoothed", start_date)
                expected_raw_dates = self.generate_expected_dates(params_, "raw", start_date)

                assert smoothed_dates == expected_smoothed_dates
                assert raw_dates == expected_raw_dates
                shutil.rmtree(issue_dir)
                start_date += timedelta(days=1)
    def test_patch_default(self, params_w_patch):
        params_w_patch["indicator"]["num_export_days"] = None
        self.mocked_patch(params_w_patch)
    def test_patch_date_set(self, params_w_patch):
        self.mocked_patch(params_w_patch)


if __name__ == '__main__':
    unittest.main()