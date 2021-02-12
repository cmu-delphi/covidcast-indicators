"""Tests for running the geo conversion functions."""

import pandas as pd
import numpy as np

from delphi_utils.geomap import GeoMapper
from delphi_hhs_facilities.geo import convert_geo, fill_missing_fips


class TestGeo:

    def test_convert_geo(self):
        gmpr = GeoMapper()
        test_input = pd.DataFrame(
            {"state": ["test"],
             "fips_code": ["01001"],
             "zip": ["01001"],
             })
        test_state_output = convert_geo(test_input, "state", gmpr)
        pd.testing.assert_series_equal(
            test_state_output.geo_id, pd.Series(["test"]), check_names=False
        )
        test_county_output = convert_geo(test_input, "county", gmpr)
        pd.testing.assert_series_equal(
            test_county_output.geo_id, pd.Series(["01001"]), check_names=False
        )
        test_msa_output = convert_geo(test_input, "msa", gmpr)
        pd.testing.assert_series_equal(
            test_msa_output.geo_id, pd.Series(["33860"]), check_names=False
        )
        test_hrr_output = convert_geo(test_input, "hrr", gmpr)
        pd.testing.assert_series_equal(
            test_hrr_output.geo_id, pd.Series(["230"]), check_names=False
        )

    def test_fill_missing_fips(self):
        gmpr = GeoMapper()
        test_input = pd.DataFrame(
            {"hospital_pk": ["test", "test2", "test3"],
             "fips_code": ["fakefips", np.nan, np.nan],
             "zip": ["01001", "01001", "00601"],
             "val1": [1.0, 5.0, 10.0],
             "val2": [2.0, 25.0, 210.0]
             })
        expected = pd.DataFrame(
            {"hospital_pk": ["test", "test2", "test3", "test3"],
             "fips_code": ["fakefips", "25013", "72001", "72141"],
             "zip": ["01001", "01001", "00601", "00601"],
             "val1": [1.0, 5.0, 0.994345718901454*10, 0.005654281098546042*10],
             "val2": [2.0, 25.0, 0.994345718901454*210.0, 0.005654281098546042*210.0]
             })
        pd.testing.assert_frame_equal(fill_missing_fips(test_input, gmpr), expected)

        # test all nans stay as nan
        test_input = pd.DataFrame(
            {"hospital_pk": ["test", "test2", "test3"],
             "fips_code": ["fakefips", np.nan, np.nan],
             "zip": ["01001", "01001", "00601"],
             "val1": [1.0, 5.0, np.nan],
             "val2": [2.0, 25.0, 210.0]
             })
        expected = pd.DataFrame(
            {"hospital_pk": ["test", "test2", "test3", "test3"],
             "fips_code": ["fakefips", "25013", "72001", "72141"],
             "zip": ["01001", "01001", "00601", "00601"],
             "val1": [1.0, 5.0, np.nan, np.nan],
             "val2": [2.0, 25.0, 0.994345718901454*210.0, 0.005654281098546042*210.0]
             })
        pd.testing.assert_frame_equal(fill_missing_fips(test_input, gmpr), expected)

        # test that populated fips or both nan is no-op
        test_input_no_missing = pd.DataFrame(
            {"hospital_pk": ["test", "test2", "test3", "test4"],
             "fips_code": ["fakefips", "testfips", "pseudofips", np.nan],
             "zip": ["01001", "01001", "00601", np.nan],
             "val": [1.0, 5.0, 10.0, 0.0]
             })
        pd.testing.assert_frame_equal(fill_missing_fips(test_input_no_missing, gmpr),
                                      test_input_no_missing)
