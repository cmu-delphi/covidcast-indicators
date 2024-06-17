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
)
from delphi_nssp.constants import (
    SIGNALS,
    NEWLINE,
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
        assert result["fips"].apply(lambda x: isinstance(x, str) and len(x) != 4).all(), "fips formatting should be 5 digits, including leading zeros if exists"

        # Check for each signal in SIGNALS
        for signal in SIGNALS:
            assert result[signal].notnull().all(), f"{signal} has rogue NaN"


if __name__ == "__main__":
    unittest.main()
