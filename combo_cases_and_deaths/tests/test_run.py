from datetime import date
from itertools import product
import pytest
import unittest
import pandas as pd

from delphi_combo_cases_and_deaths.run import extend_raw_date_range, sensor_signal, combine_usafacts_and_jhu, COLUMN_MAPPING
from delphi_combo_cases_and_deaths.handle_wip_signal import add_prefix
from delphi_utils import read_params
from delphi_combo_cases_and_deaths.constants import METRICS, SMOOTH_TYPES, SENSORS, GEO_RESOLUTIONS


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

def test_unstable_sources():
    placeholder = lambda geo: pd.DataFrame(
        [(date.today(),"pr" if geo == "state" else "72000",1,1,1,0)],
        columns="time_value geo_value value stderr sample_size direction".split())
    fetcher10 = lambda *x: placeholder(x[-1]) if x[0] == "usa-facts" else None
    fetcher01 = lambda *x: placeholder(x[-1]) if x[0] == "jhu-csse" else None
    fetcher11 = lambda *x: placeholder(x[-1])
    fetcher00 = lambda *x: None

    date_range = [date.today(), date.today()]

    for geo in "state county msa".split():
        for (fetcher, expected_size) in [
                (fetcher00, 0),
                (fetcher01, 0 if geo == "msa" else 1),
                (fetcher10, 1),
                (fetcher11, 1 if geo == "msa" else 2)
        ]:
            df = combine_usafacts_and_jhu("", geo, date_range, fetcher)
            assert df.size == expected_size * len(COLUMN_MAPPING), f"""
input for {geo}:
{fetcher('usa-facts',geo)}
{fetcher('jhu-csse',geo)}

output:
{df}
"""

class MyTestCase(unittest.TestCase):
    pass


if __name__ == '__main__':
    unittest.main()
