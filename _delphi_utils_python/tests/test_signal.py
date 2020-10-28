"""Tests for delphi_utils.signal."""
from unittest.mock import patch
import pandas as pd

from delphi_utils.signal import add_prefix, public_signal

# Constants for mocking out the call to `covidcast.metadata` within `public_signal()`.
PUBLIC_SIGNALS = ["sig1", "sig2", "sig3"]
PUBLIC_SIGNALS_FRAME = pd.DataFrame(data={"signal": PUBLIC_SIGNALS})

class TestSignal:
    """Tests for signal.py."""

    def test_add_prefix_to_all(self):
        """Tests that `add_prefix()` derives work-in-progress names for all input signals."""
        assert add_prefix(["sig1", "sig3"], True, prefix="wip_") == ["wip_sig1", "wip_sig3"]
    
    def test_add_prefix_to_specified(self):
        """Tests that `add_prefix()` derives work-in-progress names for specified signals."""
        assert add_prefix(["sig1", "sig2", "sig3"], ["sig2"], prefix="wip_") ==\
            ["sig1", "wip_sig2", "sig3"]
    
    @patch("covidcast.metadata")
    def test_add_prefix_to_non_public(self, metadata):
        """Tests that `add_prefix()` derives work-in-progress names for non-public signals."""
        metadata.return_value = PUBLIC_SIGNALS_FRAME
        assert add_prefix(["sig0", "sig1"], False, prefix="wip_") == ["wip_sig0", "sig1"]

    @patch("covidcast.metadata")
    def test_public_signal(self, metadata):
        """Tests that `public_signal()` identifies public vs. private signals."""
        metadata.return_value = PUBLIC_SIGNALS_FRAME
        assert not public_signal("sig0")
        assert public_signal("sig2")
