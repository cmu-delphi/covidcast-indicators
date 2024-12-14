import pandas as pd

from delphi_doctor_visits.geo_maps import GeoMaps
from delphi_doctor_visits.config import Config

CONFIG = Config()
DATA = pd.read_csv(
    "test_data/SYNEDI_AGG_OUTPATIENT_07022020_1455CDT.csv.gz",
    usecols=CONFIG.FILT_COLS,
    dtype=CONFIG.DTYPES,
    parse_dates=[CONFIG.DATE_COL],
    nrows=9,
)

GM = GeoMaps()


class TestGeoMap:
    def test_convert_fips(self):

        assert GM.convert_fips("23") == "00023"

    def test_county_to_msa(self):

        out, name = GM.county_to_msa(DATA)
        assert name == "cbsa_id"
        assert set(out.groups.keys()) == {"11500", "13820", "19300", "33860"}

    def test_county_to_state(self):

        out, name = GM.county_to_state(DATA)
        assert name == "state_id"
        assert set(out.groups.keys()) == {"al"}

    def test_county_to_hrr(self):

        out, name = GM.county_to_hrr(DATA)
        assert name == "hrr"
        assert set(out.groups.keys()) == {"1", "134", "144", "146", "2", "5", "6", "7", "9"}

    def test_county_to_megacounty(self):

        out, name = GM.county_to_megacounty(DATA, 100000, 10)
        assert name == "PatCountyFIPS"
        assert set(out.groups.keys()) == {
                "01001",
                "01003",
                "01005",
                "01007",
                "01009",
                "01011",
                "01013",
                "01015",
                "01017",
                "01000"
        }
