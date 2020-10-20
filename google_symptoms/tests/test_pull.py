import pytest


from delphi_google_symptoms.pull import pull_gs_data
from delphi_google_symptoms.constants import METRICS

base_url_good = "./test_data{sub_url}small_{state}symptoms_dataset.csv"

base_url_bad = {
    "missing_cols": "test_data/bad_state_missing_cols.csv",
    "invalid_fips": "test_data/bad_county_invalid_fips.csv"
}


class TestPullGoogleSymptoms:
    def test_state_good_file(self):
        df = pull_gs_data(base_url_good, METRICS, "state")
        assert (
            df.columns.values
            == ["geo_id", "timestamp", "symptom:Anosmia", "symptom:Ageusia"]
        ).all()

    def test_county_good_file(self):
        df = pull_gs_data(base_url_good, METRICS, "county")

        assert (
            df.columns.values
            == ["geo_id", "timestamp", "symptom:Anosmia", "symptom:Ageusia"]
        ).all()

    def test_missing_days(self):        
        with pytest.raises(ValueError):
            pull_gs_data(base_url_bad["missing_cols"], METRICS, "state")

    def test_invalid_fips(self):
        with pytest.raises(AssertionError):
            pull_gs_data(base_url_bad["invalid_fips"], METRICS, "county")