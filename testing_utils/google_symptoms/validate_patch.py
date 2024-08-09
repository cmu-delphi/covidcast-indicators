from typing import List, Tuple

import covidcast
import pandas as pd
from pathlib import Path
import os
import numpy as np
from itertools import product
from delphi_epidata import Epidata
from delphi_google_symptoms.constants import GEO_RESOLUTIONS, COMBINED_METRIC
from datetime import datetime


SOURCE_DIR = Path(__file__).resolve().parent

def generate_date_for_query(file_list: List[Path]) -> Tuple[str, str]:
    start_date_prefix = file_list[0].name.split("_")[0]
    end_date_prefix = file_list[-1].name.split("_")[0]
    return start_date_prefix, end_date_prefix

def check_signal_num(file_list: List[Path]):
    smoother = ["raw", "smoothed"]
    for metric, smoother, geo in product(COMBINED_METRIC, smoother, GEO_RESOLUTIONS):
        file_prefix = f"{geo}_{metric}_{smoother}"
        files_with_prefix = [f.name for f in file_list if file_prefix in f.name]
        if smoother == "raw":
            assert len(files_with_prefix) == 21
        if smoother == "smoothed":
            assert len(files_with_prefix) == 15

def compare(expected_files, actual_files):
    passed = 0
    if len(expected_files) != len(actual_files):
        print("length not equal return")
        return

    for expected_file, actual_file in zip(expected_files, actual_files):
        actual = pd.read_csv(actual_file)
        expected = pd.read_csv(expected_file)
        df_diff = expected.merge(actual, on=['geo_id'], how='left', indicator=True)
        if df_diff[df_diff["_merge"] == "both"].shape[0] == df_diff.shape[0]:
            passed += 1
        else:
            path_name = "compare_mismatch"
            os.makedirs(path_name, exist_ok=True)
            mismatch = df_diff[df_diff["_merge"] != "both"]

            mismatch.to_csv(f"{path_name}/{actual_file.name.split('.')[0]}_mismatch.csv", index=False)

    print(f"Passed: {passed} out of {len(expected_files)}")

def process_response(response, geo):
    df = pd.DataFrame.from_dict(response["epidata"])
    df["issue"] = df['issue'].astype('str')
    df.drop(
        columns=["time_type", "source", "signal", "direction", "lag", "missing_stderr", "missing_sample_size",
                 "missing_value", "geo_type"], inplace=True)
    df.rename(columns={"geo_value": "geo_id", "value": "val", "stderr": "se"}, inplace=True)
    df.replace(to_replace=[None], value=np.nan, inplace=True)
    if geo not in ["state", "nation"]:
        df["geo_id"] = df['geo_id'].astype('str')
    if geo in ["county", "msa"]:
        df["geo_id"] = df['geo_id'].str.zfill(5)
    return df

def compare_to_api(actual_files, run_type, issues=datetime.now().strftime("%Y%m%d")):
    API_KEY = os.environ.get('DELPHI_API_KEY')
    Epidata.api_key = ("epidata", API_KEY)
    covidcast.use_api_key(API_KEY)
    print(f"comparing {run_type} run with api {issues}")
    passed = 0
    start_date, end_date = generate_date_for_query(actual_files)
    signal_df_dict = dict()
    smoother = ["raw", "smoothed"]
    lag_date = 0
    for metric, smoother, geo in product(COMBINED_METRIC, smoother, GEO_RESOLUTIONS):
        signal = f"{metric}_{smoother}_search"
        response = Epidata.covidcast("google-symptoms", signal, time_type="day",
                                      geo_type=geo,
                                     time_values=Epidata.range(start_date, end_date),
                                     issues=issues,
                                      geo_value="*", as_of=None, lag=None)

        df = process_response(response, geo)

        for g, data in df.groupby("time_value"):
            data.drop(columns=["time_value"], inplace=True)
            for issue_date, issue_date_data in data.groupby("issue"):
                issue_date_data.drop(columns=["issue"], inplace=True)
                signal_df_dict[f"{g}_{geo}_{signal}"] = (issue_date, issue_date_data)

    for actual_file in actual_files:
        actual = pd.read_csv(actual_file, index_col=False, converters={'geo_id': str})
        expected = signal_df_dict.get(actual_file.name.split(".")[0], (None, pd.DataFrame()))
        issue_date, expected_df = expected
        if not expected_df.empty:
            expected_df = expected_df.reset_index(drop=True)

            df_diff = expected_df.merge(actual, on=['geo_id'], how='left', suffixes=["_api", f"_{run_type}"], indicator=True)
            if df_diff[df_diff["_merge"] == "both"].shape[0] == df_diff.shape[0]:
                if issue_date == issues["to"]:
                    passed += 1
                elif int(issue_date) < int(issues["to"]):
                    lag_date += 1
                else:
                    print(f"weird check out {signal}")
            else:
                path_name = f"{run_type}_mismatch"
                os.makedirs(path_name, exist_ok=True)
                temp = df_diff[df_diff["_merge"] != "both"]
                mismatch = temp.copy(deep=True)
                mismatch.drop(columns=["_merge"], inplace=True)
                mismatch.to_csv(f"{path_name}/{actual_file.name.split('.')[0]}_mismatch.csv", index=False)
        else:
            print(f"    {actual_file.name.split('.')[0]} not in api")

    if passed + lag_date == len(actual_files):
        print(f"    Passed({passed}) + Lagged({lag_date}) = {len(actual_files)}")
    else:
        print(f"    Passed: {passed} out of {len(actual_files)}")



if __name__ == "__main__":
    # This is a sample run; it may need adjustments to get it to work correctly

    patch_dirs = sorted(list(Path(f"{SOURCE_DIR}/patch_dir/").glob("issue*")))
    # Testing for number of signals
    for patch_dir in patch_dirs:
        patch_files = sorted(list(Path(f"{patch_dir}/google-symptoms").glob("*.csv")))
        check_signal_num(patch_files)

    # Testing for matching regular run with patch run (the enddate should match up to the last date in the patch run)
    regular_dir = sorted(list(Path(f"{SOURCE_DIR}/regular_run").glob("*.csv")))
    patch_dir = sorted(list(Path(f"{SOURCE_DIR}/patch_dir/issue_yyyymmdd/google-symptoms").glob("*.csv")))
    compare(regular_dir, patch_dir)

    # Testing against the api; The data in the api require a range of date to validate
    # as runs at the time some of the earlier date may be only available on previous issue dates
    compare_to_api(sorted(list(Path(f"{SOURCE_DIR}/fix_patch_today/issue_20240801/google-symptoms").glob(f"*.csv"))),
                   "fix_patch", Epidata.range("20240731", str(20240802)))
    for i in range(2,6):
        compare_to_api(sorted(list(Path(f"{SOURCE_DIR}/fix_patch_today/issue_2024080{i}/google-symptoms").glob(f"*.csv"))), "fix_patch", Epidata.range(str(20240800 + i - 1), str(20240801 + i)))





