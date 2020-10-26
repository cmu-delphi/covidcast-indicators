"""Tests for delphi_utils.signal."""

from delphi_utils.signal import add_prefix, public_signal

SIGNALS = ["median_home_dwell_time", "completely_home_prop", "full_time_work_prop"]

class TestSignal:
    """Tests for signal.py."""
    def test_handle_wip_signal(self):
        """Tests that `add_prefix()` derives work-in-progress signals."""
        # Test wip_signal = True
        signal_names = SIGNALS
        signal_names = add_prefix(SIGNALS, True, prefix="wip_")
        assert all(s.startswith("wip_") for s in signal_names)
        # Test wip_signal = list
        signal_names = add_prefix(SIGNALS, [SIGNALS[0]], prefix="wip_")
        assert signal_names[0].startswith("wip_")
        assert all(not s.startswith("wip_") for s in signal_names[1:])
        # Test wip_signal = False
        signal_names = add_prefix(["xyzzy", SIGNALS[0]], False, prefix="wip_")
        assert signal_names[0].startswith("wip_")
        assert all(not s.startswith("wip_") for s in signal_names[1:])

    def test_public_signal(self):
        """Tests that `public_signal()` identifies public vs. private signals."""
        assert not public_signal("junk")
        assert public_signal("covid_ag_smoothed_pct_positive")
