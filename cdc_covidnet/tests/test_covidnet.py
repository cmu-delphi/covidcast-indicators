import json
from os.path import join, exists
from tempfile import TemporaryDirectory

import numpy as np

from delphi_cdc_covidnet.api_config import APIConfig
from delphi_cdc_covidnet.covidnet import CovidNet


class TestCovidNet:

    def test_mappings(self):
        with TemporaryDirectory() as temp_dir:
            init_file = join(temp_dir, "init.json")

            # Perform the download
            CovidNet.download_mappings(
                url=APIConfig.INIT_URL,
                outfile=init_file)

            assert exists(init_file)

            with open(init_file, "r") as f_json:
                mappings = json.load(f_json)

            # Check if the used keys are in the file
            used_keys = ["catchments", "mmwr", "ages"]
            for key in used_keys:
                assert key in mappings.keys(), f"Key '{key}' missing from mappings"

            catchment_info, mmwr_info, age_info = CovidNet.read_mappings(init_file)

            # Catchment columns
            catchment_cols = ["networkid", "catchmentid", "area", "name"]
            for col in catchment_cols:
                assert col in catchment_info.columns

            # MMWR columns
            mmwr_cols = ["year", "weeknumber", "weekstart", "weekend"]
            for col in mmwr_cols:
                assert col in mmwr_info.columns
            assert mmwr_info["weekstart"].dtype == np.dtype("datetime64[ns]")
            assert mmwr_info["weekend"].dtype == np.dtype("datetime64[ns]")
            assert (mmwr_info["weekstart"] < mmwr_info["weekend"]).all()

            # Age columns
            age_cols = ["ageid", "parentid", "label"]
            for col in age_cols:
                assert col in age_info.columns
            assert (age_info["label"] == "Overall").any(), "Missing overall age-group"

    def test_hosp_data(self):
        # Download mappings file
        with TemporaryDirectory() as temp_dir:
            init_file = join(temp_dir, "init.json")
            CovidNet.download_mappings(
                url=APIConfig.INIT_URL,
                outfile=init_file)
            catchment_info, _, _ = CovidNet.read_mappings(init_file)

            # Download all state files
            states_idx = catchment_info["area"] != "Entire Network"
            num_states = states_idx.sum()

            # Non-parallel
            state_files = CovidNet.download_all_hosp_data(
                init_file, temp_dir, parallel=False)
            assert len(state_files) == num_states
            for state_file in state_files:
                assert exists(state_file)

            # Parallel
            state_files_par = CovidNet.download_all_hosp_data(
                init_file, temp_dir, parallel=True)
            assert set(state_files) == set(state_files_par)
            assert len(state_files_par) == num_states
            for state_file in state_files_par:
                assert exists(state_file)

            # Run the combining function
            hosp_df = CovidNet.read_all_hosp_data(state_files)

            # Check all used columns are there
            df_cols = ["mmwr-year", "mmwr-week", "catchment", "cumulative-rate"]
            for col in df_cols:
                assert col in hosp_df.columns, f"Column '{col}' missing from dataframe"

            # Verify we indeed have data for each state we downloaded for
            assert set(hosp_df["catchment"].unique()) == set(catchment_info.loc[states_idx, "area"])
