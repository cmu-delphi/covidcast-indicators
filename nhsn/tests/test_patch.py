import glob
import os
import tempfile
from pathlib import Path
import shutil
from unittest.mock import patch as mock_patch

import pandas as pd
from datetime import datetime, timedelta

from epiweeks import Week

from delphi_nhsn.patch import group_source_files, patch
from delphi_nhsn.constants import TOTAL_ADMISSION_COVID_API, TOTAL_ADMISSION_FLU_API
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

    def test_group_source_files(self):
        filelist = self.generate_dummy_file_names()
        processed_file_list = group_source_files(filelist)
        for file_list in processed_file_list:
            converted_file_list = [Week.fromdate(date) for date in file_list]
            assert len(converted_file_list) == len(set(converted_file_list))

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
        for date in date_list:
            custom_filename = Path(f"/tmp/{date}.csv.gz")
            test_data = pd.DataFrame(TEST_DATA)
            test_data[TOTAL_ADMISSION_COVID_API] = int(date)
            test_data[TOTAL_ADMISSION_FLU_API] = int(date)
            test_prelim_data = pd.DataFrame(PRELIM_TEST_DATA)
            test_prelim_data[TOTAL_ADMISSION_COVID_API] = int(date)
            test_prelim_data[TOTAL_ADMISSION_FLU_API] = int(date)

            test_data = test_data.head(2)
            test_data.to_csv(
                f"{TEST_DIR}/backups/{date}.csv.gz", index=False, na_rep="NA", compression="gzip"
            )
            test_prelim_data = test_data.head(2)
            test_prelim_data.to_csv(
                f"{TEST_DIR}/backups/{date}_prelim.csv.gz", index=False, na_rep="NA", compression="gzip"
            )
            file_list.append(custom_filename)
        return file_list

    def test_patch(self, params_w_patch):
        with mock_patch("delphi_nhsn.patch.read_params", return_value=params_w_patch):
            self.generate_test_source_files()
            patch(params_w_patch)

        for idx in range(7):
            patch_paths = [Path(dir) for dir in glob.glob(f"{TEST_DIR}/patch_dir_{idx}/*")]
            for patch_path in patch_paths:
                # epiweek + the index of the patch files should equal the issue date (which is set as the value of the csv)
                issue_dt = Week.fromstring(patch_path.name.replace("issue_", "")).daydate(idx).strftime("%Y%m%d")
                for patch_file in Path(patch_path / "nhsn").iterdir():
                    df = pd.read_csv(str(patch_file))
                    val = str(int(df["val"][0]))
                    assert issue_dt == val

        # clean up
        for idx in range(7):
            shutil.rmtree(f"{TEST_DIR}/patch_dir_{idx}")

        for file in glob.glob(f"{TEST_DIR}/backups/*.csv"):
            os.remove(file)





