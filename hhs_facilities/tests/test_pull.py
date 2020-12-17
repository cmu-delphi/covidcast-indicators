"""Tests for running the geo conversion functions."""

from unittest.mock import patch

import pandas as pd
import numpy as np

from delphi_utils.geomap import GeoMapper
from delphi_hhs_facilities.pull import pull_data


class TestPull:

    @patch("delphi_hhs_facilities.pull.Epidata.covid_hosp_facility")
    def test_pull(self, covid_hosp_facility):
        # test bad response
        # test values correct
        # test data types correct (fips, zip, and timestamp)
        # test nans parsed
        assert True
