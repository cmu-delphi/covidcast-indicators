"""This test is to ensure the constants are not accidentally altered."""

from delphi_nowcast import constants
from delphi_nowcast.data_containers import SensorConfig


def test_constants():
    """If any of these tests fail, please verify that the constant changes are intended.

    If any sensorization constants are changed, verify that you have updated the sensor name in
    constants.py so you do not mix the newly configured sensor values with values from previous
    configurations.
    """
    assert len(dir(constants)) == 16
    assert constants.GROUND_TRUTH_INDICATOR == SensorConfig("placeholder", "placeholder", "placeholder", 0)
    assert constants.DELAY_DISTRIBUTION == []
    assert constants.FIT_FUNC == "placeholder"
    assert constants.AR_ORDER == 3
    assert constants.AR_LAMBDA == 0.1
    assert constants.REG_SENSORS == [SensorConfig("placeholder", "placeholder", "placeholder", 0),]
    assert constants.REG_INTERCEPT is True
