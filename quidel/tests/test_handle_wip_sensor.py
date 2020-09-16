import unittest
from delphi_quidel.handle_wip_sensor import add_prefix
from delphi_quidel.constants import SENSORS


class MyTestCase(unittest.TestCase):
    def test_handle_wip_sensor(self):
        # Test wip_signal = True, Add prefix to all signals
        sensors = list(SENSORS.keys())
        signal_names = add_prefix(sensors, True, prefix="wip_")
        assert all(s.startswith("wip_") for s in signal_names)
        # Test wip_signal = list, Add prefix to signal list
        signal_names = add_prefix(sensors, [sensors[0]], prefix="wip_")
        assert signal_names[0].startswith("wip_")
        assert all(not s.startswith("wip_") for s in signal_names[1:])
        # Test wip_signal = False, Add prefix to unpublished signals
        signal_names = add_prefix(["xyzzy", sensors[0]], False, prefix="wip_")
        assert signal_names[0].startswith("wip_")
        assert all(not s.startswith("wip_") for s in signal_names[1:])


if __name__ == '__main__':
    unittest.main()
