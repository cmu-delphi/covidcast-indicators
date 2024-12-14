"""Tests for module utils."""

from datetime import date, timedelta
from freezegun import freeze_time
import pandas as pd
from delphi_utils.validator.datafetcher import FILENAME_REGEX
from delphi_utils.validator.utils import relative_difference_by_min, aggregate_frames, TimeWindow

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

class TestTimeWindow:
    """Tests for TimeWindow data class."""
    def test_init(self):
        """Test that the start date is computed correctly."""
        window = TimeWindow(date(2020, 11, 7), timedelta(days=4))
        assert window.start_date == date(2020, 11, 4)
        assert window.date_seq == [date(2020, 11, 4),
                                   date(2020, 11, 5),
                                   date(2020, 11, 6),
                                   date(2020, 11, 7)]

    @freeze_time("2020-02-14")
    def test_string_init(self):
        """Test that TimeWindows can be derived from strings."""
        window = TimeWindow.from_params("2020-08-23", 366)
        assert window.start_date == date(2019, 8, 24)

        today_window = TimeWindow.from_params("today", 14)
        assert today_window.start_date == date(2020, 2, 1) 

        latest_window = TimeWindow.from_params("today-0", 1897)
        assert latest_window.start_date == date(2014, 12, 6)
        latest_window2 = TimeWindow.from_params("today-10", 3)
        assert latest_window2.end_date == date(2020, 2, 4)
        assert latest_window2.start_date == date(2020, 2, 2)
