import unittest
from delphi_google_health.run import add_prefix, public_signal
from delphi_google_health.constants import SIGNALS


class MyTestCase(unittest.TestCase):
    def test_handle_signal_name(self):
        # Test wip_signal = True, add prefix to all signals
        signal_names = add_prefix(SIGNALS, True, prefix="wip_")
        assert all(s.startswith("wip_") for s in signal_names)
        # Test wip_signal = list, add prefix to listed signals
        signal_names = add_prefix(SIGNALS, [SIGNALS[0]], prefix="wip_")
        assert signal_names[0].startswith("wip_")
        assert all(not s.startswith("wip_") for s in signal_names[1:])
        # Test wip_signal = False, add prefix to unpublished signals
        signal_names = add_prefix(["xyzzy", SIGNALS[0]], False, prefix="wip_")
        assert signal_names[0].startswith("wip_")
        assert all(not s.startswith("wip_") for s in signal_names[1:])


if __name__ == '__main__':
    unittest.main()
