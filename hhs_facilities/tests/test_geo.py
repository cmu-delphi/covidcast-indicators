"""Tests for running the geo conversion functions."""

import pandas as pd

from delphi_utils.geomap import GeoMapper
from delphi_hhs_facilities.geo import convert_geo


class TestGeo:

    def test_convert_geo(self):
        gmpr = GeoMapper()
        test_input = pd.DataFrame(
            {"state": ["test"],
             "fips_code": ["hello"],
             "zip": ["01001"],
             })
        test_state_output = convert_geo(test_input, "state", gmpr)
        pd.testing.assert_series_equal(
            test_state_output.geo_id, pd.Series(["test"]), check_names=False
        )
        test_county_output = convert_geo(test_input, "county", gmpr)
        pd.testing.assert_series_equal(
            test_county_output.geo_id, pd.Series(["hello"]), check_names=False
        )
        test_msa_output = convert_geo(test_input, "msa", gmpr)
        pd.testing.assert_series_equal(
            test_msa_output.geo_id, pd.Series(["44140"]), check_names=False
        )
        test_hrr_output = convert_geo(test_input, "hrr", gmpr)
        pd.testing.assert_series_equal(
            test_hrr_output.geo_id, pd.Series(["230"]), check_names=False
        )

