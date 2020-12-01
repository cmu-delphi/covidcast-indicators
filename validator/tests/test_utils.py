"""Tests for module utils."""

from datetime import date
import pandas as pd
from delphi_validator.datafetcher import FILENAME_REGEX
from delphi_validator.utils import relative_difference_by_min, aggregate_frames

class TestUtils:
    """Tests for module utils."""

    def test_relative_difference_by_min(self):
        """Test basic functionality of relative_difference_by_min."""
        assert relative_difference_by_min(16, 10) == 0.6

    def test_aggregate_frames(self):
        """Test that frames are aggregated and their data is derived from the re.match objects."""
        frame_1 = pd.DataFrame({"data": list(range(10))})
        frame_2 = pd.DataFrame({"data": list(range(10, 20))})
        match_1 = FILENAME_REGEX.match("20200404_state_signal_1.csv")
        match_2 = FILENAME_REGEX.match("20200505_county_signal_2.csv")

        actual = aggregate_frames([(None, match_1, frame_1), (None, match_2, frame_2)])
        expected = pd.DataFrame({"data": list(range(20)),
                                 "geo_type": ["state"] * 10 + ["county"] * 10,
                                 "time_value": [date(2020, 4, 4)] * 10 + [date(2020, 5, 5)] * 10,
                                 "signal": ["signal_1"] * 10 + ["signal_2"] * 10
                                })
        pd.testing.assert_frame_equal(actual, expected)
