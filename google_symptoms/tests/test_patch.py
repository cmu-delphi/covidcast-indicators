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
        max_expected_lag = lag_converter(params_["validation"]["common"].get("max_expected_lag", {"all": 4}))
        global_max_expected_lag = max(list(max_expected_lag.values()))

        if params_["indicator"].get("num_export_days"):
            num_export_days = params_["indicator"]["num_export_days"]
        else:
            num_export_days = params_["validation"]["common"].get("span_length", 14) + global_max_expected_lag

        # mimic date generate as if the issue date was "today"
        query_start_date, query_end_date = generate_query_dates(
            FULL_BKFILL_START_DATE,
            issue_date,
            num_export_days,
            False
        )
        # the smoother in line 82-88 filters out prev seven days
        export_start_date = query_start_date + timedelta(days=6) if smoother == "smoothed" else query_start_date
        export_end_date = query_end_date - timedelta(days=global_max_expected_lag)
        num_export_days = (export_end_date - export_start_date).days + 1

        return sorted([export_start_date + timedelta(days=x) for x in range(num_export_days)])

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
                    end_date_w_lag = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=4)).strftime("%Y-%m-%d")
                    return df[(df["date"] >= start_date) & \
                              (df["date"] <= end_date_w_lag)]
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