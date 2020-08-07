import unittest
from delphi_NAME.handle_wip_signal import add_prefix
from delphi_NAME.run import SIGNALS
from delphi_utils import read_params

wip_signal = read_params()["wip_signal"]


def test_handle_wip_signal():
    assert isinstance(wip_signal, (list, bool)) or wip_signal == "", "Supply True | False or "" or [] | list()"
    if isinstance(wip_signal, list):
        assert set(wip_signal).issubset(set(SIGNALS)), "signal in params don't belong in the registry"
    updated_signal_names = add_prefix(SIGNALS, wip_signal, prefix='wip_')
    assert (len(updated_signal_names) >= len(SIGNALS))


class MyTestCase(unittest.TestCase):
    pass


if __name__ == '__main__':
    unittest.main()
