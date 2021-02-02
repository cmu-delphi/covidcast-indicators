"""Tests for running combo cases and deaths indicator."""
from datetime import date
from itertools import product
import unittest
import pandas as pd

from delphi_combo_cases_and_deaths.run import (
    extend_raw_date_range,
    sensor_signal,
    combine_usafacts_and_jhu,
    COLUMN_MAPPING)
from delphi_combo_cases_and_deaths.constants import METRICS, SMOOTH_TYPES, SENSORS


def test_issue_dates():
    """The smoothed value for a particular date is computed from the raw
    values for a span of dates. We want users to be able to see in the
    API all the raw values that went into the smoothed computation,
    for transparency and peer review. This means that each issue
    should contain more days of raw data than smoothed data.
    """
    reference_dr = [date.today(), date.today()]
    params = {'date_range': reference_dr}
    n_changed = 0
    variants = [sensor_signal(metric, sensor, smoother) for
                metric, sensor, smoother in
                product(METRICS, SENSORS, SMOOTH_TYPES)]
    variants_changed = []
    for sensor_name, _ in variants:
        dr = extend_raw_date_range(params, sensor_name)
        if dr[0] != reference_dr[0]:
            n_changed += 1
            variants_changed.append(sensor_name)
    assert n_changed == len(variants) / 2, f"""
Raw variants should post more days than smoothed.
All variants: {variants}
Date-extended variants: {variants_changed}
"""


def test_unstable_sources():
    """Verify that combine_usafacts_and_jhu assembles the combined data
    frame correctly for all cases where 0, 1, or both signals are
    available.
    """
    placeholder = lambda geo: pd.DataFrame(
        [(date.today(),"pr" if geo == "state" else "72000",1,1,1)],
        columns="time_value geo_value value stderr sample_size".split())
    fetcher10 = lambda *x: placeholder(x[-1]) if x[0] == "usa-facts" else None
    fetcher01 = lambda *x: placeholder(x[-1]) if x[0] == "jhu-csse" else None
    fetcher11 = lambda *x: placeholder(x[-1])
    fetcher00 = lambda *x: None

    date_range = [date.today(), date.today()]

    for geo in ["state", "county", "msa", "nation", "hhs"]:
        for (fetcher, expected_size) in [
                (fetcher00, 0),
                (fetcher01, 0 if geo == "msa" else 1),
                (fetcher10, 1),
                (fetcher11, 1 if geo in ["msa", "nation", "hhs"] else 2)
        ]:
            df = combine_usafacts_and_jhu("", geo, date_range, fetcher)
            assert df.size == expected_size * len(COLUMN_MAPPING), f"""
Wrong number of rows in combined data frame for the number of available signals.

input for {geo}:
{fetcher('usa-facts',geo)}
{fetcher('jhu-csse',geo)}

output:
{df}

expected rows: {expected_size}
"""

class MyTestCase(unittest.TestCase):
    pass


if __name__ == '__main__':
    unittest.main()
