from datetime import datetime, date
import json
import unittest
from unittest.mock import patch, MagicMock
import tempfile
import os
import time
from datetime import datetime
import pdb
import pandas as pd
import pandas.api.types as ptypes

from delphi_nssp.pull import (
    pull_nssp_data,
    secondary_pull_nssp_data,
    pull_with_socrata_api,
)
from delphi_nssp.constants import (
    NEWLINE,
    SECONDARY_COLS_MAP,
    SECONDARY_KEEP_COLS,
    SECONDARY_SIGNALS_MAP,
    SECONDARY_TYPE_DICT,
    SIGNALS,
    SIGNALS_MAP,
    TYPE_DICT,
)


class TestPullNSSPData(unittest.TestCase):
    @patch("delphi_nssp.pull.Socrata")
    def test_pull_nssp_data(self, mock_socrata):
        # Load test data
        with open("test_data/page.txt", "r") as f:
            test_data = json.load(f)

        # Mock Socrata client and its get method
        mock_client = MagicMock()
        mock_client.get.side_effect = [test_data, []]  # Return test data on first call, empty list on second call
        mock_socrata.return_value = mock_client

        # Call function with test token
        test_token = "test_token"
        result = pull_nssp_data(test_token)
        print(result)

        # Check that Socrata client was initialized with correct arguments
        mock_socrata.assert_called_once_with("data.cdc.gov", test_token)

        # Check that get method was called with correct arguments
        mock_client.get.assert_any_call("rdmq-nq56", limit=50000, offset=0)

        # Check result
        assert result["timestamp"].notnull().all(), "timestamp has rogue NaN"
        assert result["geography"].notnull().all(), "geography has rogue NaN"
        assert result["county"].notnull().all(), "county has rogue NaN"
        assert result["fips"].notnull().all(), "fips has rogue NaN"
        assert result["fips"].apply(lambda x: isinstance(x, str) and len(x) != 4).all(), "fips formatting should always be 5 digits; include leading zeros if aplicable"

        # Check for each signal in SIGNALS
        for signal in SIGNALS:
            assert result[signal].notnull().all(), f"{signal} has rogue NaN"

    @patch("delphi_nssp.pull.Socrata")
    def test_secondary_pull_nssp_data(self, mock_socrata):
        # Load test data
        with open("test_data/secondary_page.txt", "r") as f:
            test_data = json.load(f)

        # Mock Socrata client and its get method
        mock_client = MagicMock()
        mock_client.get.side_effect = [test_data, []]  # Return test data on first call, empty list on second call
        mock_socrata.return_value = mock_client

        # Call function with test token
        test_token = "test_token"
        result = secondary_pull_nssp_data(test_token)
        # print(result)

        # Check that Socrata client was initialized with correct arguments
        mock_socrata.assert_called_once_with("data.cdc.gov", test_token)

        # Check that get method was called with correct arguments
        mock_client.get.assert_any_call("7mra-9cq9", limit=50000, offset=0)

        for col in SECONDARY_KEEP_COLS:
            assert result[col].notnull().all(), f"{col} has rogue NaN"

        assert result[result['geo_value'].str.startswith('Region') ].empty, "'Region ' need to be removed from geo_value for geo_type 'hhs'"
        assert (result[result['geo_type'] == 'nation']['geo_value'] == 'National').all(), "All rows with geo_type 'nation' must have geo_value 'National'"


if __name__ == "__main__":
    unittest.main()
