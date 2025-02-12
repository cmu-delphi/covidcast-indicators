import os
from collections import defaultdict
from pathlib import Path
import shutil
from unittest.mock import patch as mock_patch
import re
import pandas as pd
from datetime import datetime, timedelta

import pytest
from epiweeks import Week

from delphi_nhsn.patch import filter_source_files, patch
from delphi_nhsn.constants import TOTAL_ADMISSION_COVID_COL, TOTAL_ADMISSION_FLU_COL, \
    NUM_HOSP_REPORTING_FLU_COL, NUM_HOSP_REPORTING_COVID_COL, GEOS, TOTAL_ADMISSION_COVID, TOTAL_ADMISSION_FLU, \
    NUM_HOSP_REPORTING_COVID, NUM_HOSP_REPORTING_FLU, NUM_HOSP_REPORTING_RSV_COL, TOTAL_ADMISSION_RSV_COL
from conftest import TEST_DATA, PRELIM_TEST_DATA, TEST_DIR

class TestPatch:

    def generate_date_list(self, start_date, end_date):
        # Generate a list of dates
        date_list = []
        current_date = start_date

        while current_date <= end_date:
            date_list.append(current_date.strftime('%Y%m%d'))
            current_date += timedelta(days=1)
        return date_list

    def generate_dummy_file_names(self):
        start_date = datetime(2024, 8, 1)
        end_date = datetime(2024, 8, 4)
        date_list_part1 = self.generate_date_list(start_date, end_date)

        start_date = datetime(2024, 9, 6)
        end_date = datetime(2024, 9, 10)
        date_list_part2 = self.generate_date_list(start_date, end_date)

        start_date = datetime(2024, 10, 6)
        end_date = datetime(2024, 10, 15)
        date_list_part3 = self.generate_date_list(start_date, end_date)

        start_date = datetime(2024, 11, 16)
        end_date = datetime(2024, 11, 22)
        date_list_part4 = self.generate_date_list(start_date, end_date)

        date_list = date_list_part1 + date_list_part2 + date_list_part3 + date_list_part4

        file_list = []
        for date in date_list:
            custom_filename = Path(f"/tmp/{date}.csv.gz")
            file_list.append(custom_filename)
        return file_list

    def test_filter_source_files(self):
        filelist = self.generate_dummy_file_names()
        epiweek_dict = defaultdict(list)
        for file in filelist:
            issue_dt = datetime.strptime(file.name.split(".")[0], "%Y%m%d")
            issue_epiweek = Week.fromdate(issue_dt)
            epiweek_dict[issue_epiweek].append(issue_dt)
        patch_issue_list = filter_source_files(filelist)
        for file in patch_issue_list:
            issue_dt = datetime.strptime(file.name.split(".")[0], "%Y%m%d")
            issue_epiweek = Week.fromdate(issue_dt)
            assert max(epiweek_dict[issue_epiweek]) == issue_dt

    def generate_test_source_files(self):
        start_date = datetime(2024, 8, 1)
        end_date = datetime(2024, 8, 4)
        date_list_part1 = self.generate_date_list(start_date, end_date)

        start_date = datetime(2024, 9, 6)
        end_date = datetime(2024, 9, 10)
        date_list_part2 = self.generate_date_list(start_date, end_date)

        start_date = datetime(2024, 11, 16)
        end_date = datetime(2024, 11, 22)
        date_list_part4 = self.generate_date_list(start_date, end_date)

        date_list = date_list_part1 + date_list_part2 + date_list_part4

        file_list = []
        prelim_file_list = []
        for date in date_list:
            custom_filename = f"{TEST_DIR}/backups/{date}.csv.gz"
            custom_filename_prelim = f"{TEST_DIR}/backups/{date}_prelim.csv.gz"
            test_data = pd.DataFrame(TEST_DATA)
            test_data[TOTAL_ADMISSION_COVID_COL] = int(date)
            test_data[TOTAL_ADMISSION_FLU_COL] = int(date)
            test_data[TOTAL_ADMISSION_RSV_COL] = int(date)
            test_data[NUM_HOSP_REPORTING_COVID_COL] = int(date)
            test_data[NUM_HOSP_REPORTING_FLU_COL] = int(date)
            test_data[NUM_HOSP_REPORTING_RSV_COL] = int(date)
            test_prelim_data = pd.DataFrame(PRELIM_TEST_DATA)
            test_prelim_data[TOTAL_ADMISSION_COVID_COL] = int(date)
            test_prelim_data[TOTAL_ADMISSION_FLU_COL] = int(date)
            test_prelim_data[TOTAL_ADMISSION_RSV_COL] = int(date)
            test_prelim_data[NUM_HOSP_REPORTING_COVID_COL] = int(date)
            test_prelim_data[NUM_HOSP_REPORTING_FLU_COL] = int(date)
            test_prelim_data[NUM_HOSP_REPORTING_RSV_COL] = int(date)

            test_data = test_data.head(3)
            test_data.to_csv(
                custom_filename, index=False, na_rep="NA", compression="gzip"
            )
            test_prelim_data = test_data.head(3)
            test_prelim_data.to_csv(
                custom_filename_prelim, index=False, na_rep="NA", compression="gzip"
            )
            file_list.append(custom_filename)
            prelim_file_list.append(custom_filename_prelim)
        return file_list, prelim_file_list

    def test_patch(self, params_w_patch):
        with mock_patch("delphi_nhsn.patch.read_params", return_value=params_w_patch):
            file_list, prelim_file_list = self.generate_test_source_files()
            patch(params_w_patch)

            for issue_path in Path(f"{TEST_DIR}/patch_dir").glob("issue*"):
                issue_dt_str = issue_path.name.replace("issue_", "")
                for file in Path(issue_path / "nhsn").iterdir():
                    df = pd.read_csv(file)
                    assert issue_dt_str == str(int(df["val"][0]))

        # clean up
        for file in Path(f"{TEST_DIR}/patch_dir").glob("issue*"):
            shutil.rmtree(file)

        for file in file_list:
            os.remove(file)

        for file in prelim_file_list:
            os.remove(file)

    def test_patch_incomplete_file(self, params_w_patch):
        os.makedirs(params_w_patch["patch"]["patch_dir"], exist_ok=True)
        issue_date = "20241119"
        existing_signals = [TOTAL_ADMISSION_COVID, TOTAL_ADMISSION_FLU]
        backup_dir = params_w_patch.get("common").get("backup_dir")
        shutil.copy(f"{TEST_DIR}/test_data/{issue_date}.csv.gz", backup_dir)

        with mock_patch("delphi_nhsn.patch.read_params", return_value=params_w_patch):
            patch(params_w_patch)

            files = list(Path(f"{TEST_DIR}/patch_dir/issue_{issue_date}/nhsn").glob("*.csv"))
            dates = set([re.search(r"\d{6}", file.name).group() for file in files])
            assert len(files) == len(GEOS) * len(existing_signals) * len(dates)
        # clean up
        for file in Path(f"{TEST_DIR}/patch_dir").glob("issue*"):
            shutil.rmtree(file)





