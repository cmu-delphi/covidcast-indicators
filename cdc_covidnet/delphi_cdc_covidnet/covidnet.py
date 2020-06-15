"""
Generate COVID-NET sensors.

Author: Eu Jing Chua
Created: 2020-06-12
"""

import json
import logging
import os
from multiprocessing import cpu_count, Pool

import requests
import pandas as pd

from .config import Config

class CovidNet:
    """
    Methods for downloading and loading COVID-NET data
    """

    @staticmethod
    def download_mappings(
            url=Config.API_INIT_URL,
            outfile="./init.json"):
        """
        Downloads the JSON file with all mappings (age, mmwr, catchments etc.) to disk

        Args:
            url: The API URL to GET from
            outfile: The output JSON file to write to
        """

        params = {"appVersion": "Public"}
        data = requests.get(url, params).json()
        with open(outfile, "w") as f_json:
            json.dump(data, f_json)

    @staticmethod
    def read_mappings(infile):
        """
        Reads the mappings JSON file from disk to produce formatted
        pd.DataFrame for relevant mappings

        Args:
            infile: Mappings JSON file

        Returns:
            age_info: Age-related mappings
            mmwr_info: Date-related mappings
            catchment_info: Geography-related mappings
        """

        with open(infile, "r") as f_json:
            data = json.load(f_json)

        # Network, catchment & area mappings
        catchment_info = pd.DataFrame.from_records(data["catchments"])

        # MMWR date mappings
        mmwr_info = pd.DataFrame.from_records(data["mmwr"], columns=Config.API_MMWR_COLS)
        mmwr_info["weekstart"] = pd.to_datetime(mmwr_info["weekstart"])
        mmwr_info["weekend"] = pd.to_datetime(mmwr_info["weekend"])

        # Age mappings
        age_info = pd.DataFrame.from_records(data["ages"], columns=Config.API_AGE_COLS)

        return catchment_info, mmwr_info, age_info

    @staticmethod
    def download_hosp_data(
            network_id, catchment_id,
            age_groups, seasons,
            outfile,
            url=Config.API_HOSP_URL):
        """
        Downloads hospitalization data to disk for a particular network or state
        Refer to catchment_info for network & catchment ID mappings
        Refer to age_info for age-group mappings
        Seasons are enumerated in original mappings JSON file

        Args:
            network_id: Network ID of intended network / state
            catchment_id: Catchment ID of intended network / state
            age_groups: List of age-group IDs to request for
            seasons: List of season IDs to request for
            outfile: JSON file to write the results to
            url: The API URL to POST to for downloading hospitalization data
        """

        download_params = {
            "AppVersion": "Public",
            "networkid": network_id,
            "catchmentid": catchment_id,
            "seasons": [{"ID": season_id} for season_id in seasons],
            "agegroups": [{"ID": ag_id} for ag_id in age_groups]
        }
        data = requests.post(url, json=download_params).json()

        with open(outfile, "w") as f_json:
            json.dump(data, f_json)

    @staticmethod
    def download_all_hosp_data(mappings_file, cache_path, parallel=False):
        """
        Downloads hospitalization data for all states listed in the mappings JSON file to disk.

        Args:
            mappings_file: Mappings JSON file
            cache_path: Cache directory to write all state hosp. JSON files to
            parallel: Download each file in parallel

        Returns:
            List of all downloaded JSON filenames (including the cache_path)
        """

        catchment_info, _, age_info = CovidNet.read_mappings(mappings_file)

        # By state
        states_idx = catchment_info["area"] != "Entire Network"
        args = catchment_info.loc[states_idx, ["networkid", "catchmentid"]]

        # All age groups
        age_groups = list(age_info.loc[age_info["label"] == "Overall", "ageid"])

        # Set up arguments for download, and file names for return
        state_files = []
        state_args = []
        for nid, cid in args.itertuples(index=False, name=None):
            outfile = os.path.join(cache_path, f"networkid_{nid}_catchmentid_{cid}.json")
            state_files.append(outfile)
            args = (nid, cid, age_groups, Config.API_SEASONS, outfile, Config.API_HOSP_URL)
            state_args.append(args)

        # Download all state files
        if parallel:
            # Originally used context-manager API, but does not work well with pytest-cov
            # https://pytest-cov.readthedocs.io/en/latest/subprocess-support.html#if-you-use-multiprocessing-pool
            # However seems to still produce .coverage.<HOSTNAME>... files on python 3.8 at least
            pool = Pool(min(10, cpu_count()))
            try:
                pool.starmap(CovidNet.download_hosp_data, state_args)
            finally:
                pool.close()
                pool.join()
        else:
            for args in state_args:
                CovidNet.download_hosp_data(*args)
                logging.debug("Downloading for nid=%s, cid=%s", args[0], args[1])

        return state_files

    @staticmethod
    def read_all_hosp_data(state_files):
        """
        Read and combine hospitalization JSON files for each state into a pd.DataFrame

        Args:
            state_files: List of hospitalization JSON files for each state to read from disk

        Returns:
            Single pd.DataFrame with all the hospitalization data combined
        """

        dfs = []
        for state_file in state_files:
            # Read json
            with open(state_file, "r") as f_json:
                data = json.load(f_json)["datadownload"]

            # Make dataframe out of json
            state_df = pd.DataFrame.from_records(data).astype(Config.API_HOSP_DTYPES)
            dfs.append(state_df)

        # Combine dataframes
        return pd.concat(dfs)
