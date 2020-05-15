import pytest

from os.path import join

from delphi_google_health.api import (
    GoogleHealthTrends,
    get_counts_states,
    get_counts_dma,
    _get_counts_geoid,
    _api_data_to_df,
    _load_cached_file,
    _write_cached_file,
)


class TestGoogleHealthTrends:
    def test_class(self):

        assert True
