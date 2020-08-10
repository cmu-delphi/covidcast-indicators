import unittest
from delphi_NAME.handle_wip_signal import add_prefix
from delphi_NAME.run import SIGNALS
from delphi_utils import read_params

wip_signal = read_params()["wip_signal"]


def test_handle_wip_signal():
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


class MyTestCase(unittest.TestCase):
    pass


if __name__ == '__main__':
    unittest.main()
