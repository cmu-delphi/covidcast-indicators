import pandas as pd

from delphi_utils import read_params
from delphi_cdc_covidnet.api_config import APIConfig
from delphi_cdc_covidnet.geo_maps import GeoMaps

params = read_params()

class TestGeoMaps:
    geomap = GeoMaps(params["static_file_dir"])

    def test_state_name_to_abbr(self):
        # Mappings of the 14 participating states on 06/15/2020
        state_abbr = [
            ("California", "CA"),
            ("Colorado", "CO"),
            ("Connecticut", "CT"),
            ("Georgia", "GA"),
            ("Maryland", "MD"),
            ("Minnesota", "MN"),
            ("New Mexico", "NM"),
            ("New York", "NY"),
            ("Oregon", "OR"),
            ("Tennessee", "TN"),
            ("Iowa", "IA"),
            ("Michigan", "MI"),
            ("Ohio", "OH"),
            ("Utah", "UT")
        ]

        state_df = pd.DataFrame(state_abbr, columns=[APIConfig.STATE_COL, "abbr"])

        # Perform mapping
        state_df = self.geomap.state_name_to_abbr(state_df)

        # Check that the mapping was right
        assert (state_df[APIConfig.STATE_COL].str.len() == 2).all()
        assert (state_df[APIConfig.STATE_COL] == state_df["abbr"]).all()
