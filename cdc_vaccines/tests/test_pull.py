"""Tests for running the CDC Vaccine indicator."""
import pytest
import logging
from unittest.mock import patch

import pandas as pd
import numpy as np
from delphi_cdc_vaccines.pull import pull_cdcvacc_data

from test_run import local_fetch

BASE_URL_GOOD = "test_data/small_pull.csv"

BASE_URL_BAD = {
    "missing_days": "test_data/bad_missing_days.csv",
    "missing_cols": "test_data/bad_missing_cols.csv",
    "extra_cols": "test_data/bad_extra_cols.csv"
}

TEST_LOGGER = logging.getLogger()

class TestPullCDCVaccines:
    """Tests for the `pull_cdcvacc_data()` function."""
    def test_good_file(self):
        """Test the expected output from a smaller file."""
        df = pull_cdcvacc_data(BASE_URL_GOOD, TEST_LOGGER)
        expected_df = pd.DataFrame({
            "fips": ["00000","00000","32013","32013","48305","48305"],
            "timestamp": [pd.Timestamp("2021-08-25"), pd.Timestamp("2021-08-26"),
                          pd.Timestamp("2021-08-25"), pd.Timestamp("2021-08-26"),
                          pd.Timestamp("2021-08-25"), pd.Timestamp("2021-08-26")],
            
            "incidence_counts_tot_vaccine": [np.nan,789625.0,np.nan,5537.0,np.nan,0.0],
            "incidence_counts_tot_vaccine_12P": [np.nan,789591.0,np.nan,5535.0,np.nan,0.0],
            "incidence_counts_tot_vaccine_18P": [np.nan,733809.0,np.nan,5368.0,np.nan,0.0],
            "incidence_counts_tot_vaccine_65P": [np.nan,55620.0,np.nan,1696.0,np.nan,0.0],
            "incidence_counts_part_vaccine": [np.nan,1119266.0,np.nan,6293.0,np.nan,0.0],
            "incidence_counts_part_vaccine_12P": [np.nan,1119203.0,np.nan,6290.0,np.nan,0.0],
            "incidence_counts_part_vaccine_18P": [np.nan,1035082.0,np.nan,6014.0,np.nan,0.0],
            "incidence_counts_part_vaccine_65P": [np.nan,75596.0,np.nan,1877.0,np.nan,0.0],



            "cumulative_counts_tot_vaccine": [np.nan,789625.0,np.nan,5537.0,np.nan,0.0],
            "cumulative_counts_tot_vaccine_12P": [np.nan,789591.0,np.nan,5535.0,np.nan,0.0],
            "cumulative_counts_tot_vaccine_18P": [np.nan,733809.0,np.nan,5368.0,np.nan,0.0],
            "cumulative_counts_tot_vaccine_65P": [np.nan,55620.0,np.nan,1696.0,np.nan,0.0],
            "cumulative_counts_part_vaccine": [np.nan,1119266.0,np.nan,6293.0,np.nan,0.0],
            "cumulative_counts_part_vaccine_12P": [np.nan,1119203.0,np.nan,6290.0,np.nan,0.0],
            "cumulative_counts_part_vaccine_18P": [np.nan,1035082.0,np.nan,6014.0,np.nan,0.0],
            "cumulative_counts_part_vaccine_65P": [np.nan,75596.0,np.nan,1877.0,np.nan,0.0]},
            
            index=[0, 1, 2, 3, 4, 5])
        # sort since rows order doesn't matter
        pd.testing.assert_frame_equal(df.sort_index(), expected_df.sort_index())

    def test_missing_days(self):
        """Test if error is raised when there are missing days."""
        with pytest.raises(ValueError):
            pull_cdcvacc_data(
                BASE_URL_BAD["missing_days"], TEST_LOGGER
            )

    def test_missing_cols(self):
        """Test if error is raised when there are missing columns."""
        with pytest.raises(ValueError):
            pull_cdcvacc_data(
                BASE_URL_BAD["missing_cols"],TEST_LOGGER
            )
