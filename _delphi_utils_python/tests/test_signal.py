"""Tests for delphi_utils.signal."""
import pandas as pd
from unittest.mock import patch

from delphi_utils.signal import add_prefix, public_signal

# Constants for mocking out the call to `covidcast.metadata` within `public_signal()`.
SIGNALS = ["sig1", "sig2", "sig3"]
SIGNALS_FRAME = pd.DataFrame(data={"signal": SIGNALS})

class TestSignal:
    """Tests for signal.py."""

    @patch("covidcast.metadata")
    def test_handle_wip_signal(self, metadata):
        """Tests that `add_prefix()` derives work-in-progress signals."""
        metadata.return_value = SIGNALS_FRAME

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

    @patch("covidcast.metadata")
    def test_public_signal(self, metadata):
        """Tests that `public_signal()` identifies public vs. private signals."""
        metadata.return_value = SIGNALS_FRAME

        assert not public_signal("sig0")
        assert public_signal("sig2")
