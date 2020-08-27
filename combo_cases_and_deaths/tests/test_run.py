from datetime import date
from itertools import product
import pytest
import unittest

from delphi_combo_cases_and_deaths.run import extend_raw_date_range, sensor_signal
from delphi_combo_cases_and_deaths.handle_wip_signal import add_prefix
from delphi_utils import read_params
from delphi_combo_cases_and_deaths.constants import *


def test_issue_dates():
    reference_dr = [date.today(), date.today()]
    params = {'date_range': reference_dr}
    n_changed = 0
    variants = [sensor_signal(metric, sensor, smoother) for
                metric, sensor, smoother in
                product(METRICS, SENSORS, SMOOTH_TYPES)]
    variants_changed = []
    for sensor_name, signal in variants:
        dr = extend_raw_date_range(params, sensor_name)
        if dr[0] != reference_dr[0]:
            n_changed += 1
            variants_changed.append(sensor_name)
    assert n_changed == len(variants) / 2, f"""Raw variants should post more days than smoothed.
All variants: {variants}
Date-extended variants: {variants_changed}
"""


def test_handle_wip_signal():

    signal_list = [sensor_signal(metric, sensor, smoother)[1]
                   for (metric, sensor, smoother) in
                   product(METRICS, SENSORS, SMOOTH_TYPES)]

    # Test wip_signal = True (all signals should receive prefix)
    signal_names = add_prefix(signal_list, True, prefix="wip_")
    assert all(s.startswith("wip_") for s in signal_names)

    # Test wip_signal = list (only listed signals should receive prefix)
    signal_names = add_prefix(signal_list, [signal_list[0]], prefix="wip_")
    print(signal_names)
    assert signal_names[0].startswith("wip_")
    assert all(not s.startswith("wip_") for s in signal_names[1:])

    # Test wip_signal = False (only unpublished signals should receive prefix)
    signal_names = add_prefix(["xyzzy", signal_list[0]], False, prefix="wip_")
    assert signal_names[0].startswith("wip_")
    assert all(not s.startswith("wip_") for s in signal_names[1:])


class MyTestCase(unittest.TestCase):
    pass


if __name__ == '__main__':
    unittest.main()
