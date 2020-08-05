from datetime import date
from itertools import product
import pytest

from delphi_combo_cases_and_deaths.run import extend_raw_date_range,sensor_signal,METRICS,SENSORS,SMOOTH_TYPES

def test_issue_dates():
    reference_dr = [date.today(),date.today()]
    params = {'date_range': reference_dr}
    n_changed = 0
    variants = [sensor_signal(metric, sensor, smoother) for
                metric, sensor, smoother in
                product(METRICS,SENSORS,SMOOTH_TYPES)]
    variants_changed = []
    for sensor_name,signal in variants:
        dr = extend_raw_date_range(params, sensor_name)
        if dr[0] != reference_dr[0]:
            n_changed += 1
            variants_changed.append(sensor_name)
    assert n_changed == len(variants) / 2, f"""Raw variants should post more days than smoothed.
All variants: {variants}
Date-extended variants: {variants_changed}
"""
