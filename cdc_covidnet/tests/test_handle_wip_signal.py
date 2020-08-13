import unittest
from delphi_cdc_covidnet.update_sensor import add_prefix
from delphi_cdc_covidnet.constants import *

def test_handle_wip_signal():
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
    assert all(s.startswith("wip_") for s in signal_names[1:])


class MyTestCase(unittest.TestCase):
    pass


if __name__ == '__main__':
    unittest.main()
