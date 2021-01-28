"""Tests for delphi_utils.signal."""
from unittest.mock import patch

import pandas as pd
import pytest
from delphi_utils.signal import add_prefix

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

    def test_invalid_prefix_input(self):
        """Tests that `add_prefix()` raises a ValueError when invalid input is given."""
        with pytest.raises(ValueError):
            add_prefix(None, None)

    def test_add_no_prefix(self):
        """Tests that `add_prefix()` doesn't affect signals if `wip_signals` is False or ''."""
        assert add_prefix(["sig0", "sig1"], False, prefix="wip_") == ["sig0", "sig1"]
        assert add_prefix(["sig0", "sig1"], "", prefix="wip_") == ["sig0", "sig1"]
