import unittest
from delphi_NAME.handle_wip_signal import add_prefix
from delphi_NAME.run import SIGNALS
from delphi_utils import read_params



def test_handle_wip_signal():
    # Test wip_signal = True (all signals should receive prefix)
    signal_names = add_prefix(SIGNALS, True, prefix="wip_")
    assert all(s.startswith("wip_") for s in signal_names)
    # Test wip_signal = list (only listed signals should receive prefix)
    signal_names = add_prefix(SIGNALS, [SIGNALS[0]], prefix="wip_")
    assert signal_names[0].startswith("wip_")
    assert all(not s.startswith("wip_") for s in signal_names[1:])
    # Test wip_signal = False (only unpublished signals should receive prefix)
    signal_names = add_prefix(["xyzzy", SIGNALS[0]], False, prefix="wip_")
    assert signal_names[0].startswith("wip_")
    assert all(not s.startswith("wip_") for s in signal_names[1:])


class MyTestCase(unittest.TestCase):
    pass


if __name__ == '__main__':
    unittest.main()
