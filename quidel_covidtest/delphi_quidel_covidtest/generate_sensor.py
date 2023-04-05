# -*- coding: utf-8 -*-
"""Functions to help generate sensor for different geographical levels."""
import pandas as pd
from .data_tools import (fill_dates, raw_positive_prop,
                         smoothed_positive_prop,
                         smoothed_tests_per_device,
                         raw_tests_per_device,
                         remove_null_samples)
from .geo_maps import add_megacounties

from .constants import (MIN_OBS, POOL_DAYS)

def generate_sensor_for_nonparent_geo(state_groups, res_key, smooth, device,
                                      first_date, last_date, suffix):
    """
    Fit over geo resolutions that don't use a parent state (nation/hhs/state).

    Args:
        state_groups: pd.groupby.generic.DataFrameGroupBy
        res_key: name of geo column
        smooth: bool
            Consider raw or smooth
        device: bool
            Consider test_per_device or pct_positive
        suffix: str
            Indicate the age group
    Returns:
        df: pd.DataFrame
    """
    state_df = pd.DataFrame(columns=["geo_id", "val", "se", "sample_size", "timestamp"])
    state_list = list(state_groups.groups.keys())
    for state in state_list:
        state_group = state_groups.get_group(state)
        state_group = state_group.drop(columns=[res_key])
        state_group.set_index("timestamp", inplace=True)
        state_group = fill_dates(state_group, first_date, last_date)

        # smoothed test per device
        if device & smooth:
            stat, se, sample_size = smoothed_tests_per_device(
                devices=state_group[f"numUniqueDevices_{suffix}"].values,
                tests=state_group[f'totalTest_{suffix}'].values,
                min_obs=MIN_OBS, pool_days=POOL_DAYS)
        # raw test per device
        elif device & (not smooth):
            stat, se, sample_size = raw_tests_per_device(
                devices=state_group[f"numUniqueDevices_{suffix}"].values,
                tests=state_group[f'totalTest_{suffix}'].values,
                min_obs=MIN_OBS)
        # smoothed pct positive
        elif (not device) & smooth:
            stat, se, sample_size = smoothed_positive_prop(
                tests=state_group[f'totalTest_{suffix}'].values,
                positives=state_group[f'positiveTest_{suffix}'].values,
                min_obs=MIN_OBS, pool_days=POOL_DAYS)
            stat = stat * 100
        # raw pct positive
        else:
            stat, se, sample_size = raw_positive_prop(
                tests=state_group[f'totalTest_{suffix}'].values,
                positives=state_group[f'positiveTest_{suffix}'].values,
                min_obs=MIN_OBS)
            stat = stat * 100

        se = se * 100
        state_df = pd.concat([
            state_df,
            pd.DataFrame({"geo_id": state,
                         "timestamp": state_group.index,
                         "val": stat,
                         "se": se,
                         "sample_size": sample_size})
        ])
    return remove_null_samples(state_df)

def generate_sensor_for_parent_geo(state_groups, data, res_key, smooth,
                                   device, first_date, last_date, suffix):
    """
    Fit over geo resolutions that use a parent state (county/hrr/msa).

    Args:
        data: pd.DataFrame
        res_key: "fips", "cbsa_id" or "hrrnum"
        smooth: bool
            Consider raw or smooth
        device: bool
            Consider test_per_device or pct_positive
        suffix: str
            Indicate the age group
    Returns:
        df: pd.DataFrame
    """
    has_parent = True
    res_df = pd.DataFrame(columns=["geo_id", "val", "se", "sample_size"])
    if res_key == "fips": # Add rest-of-state report for county level
        data = add_megacounties(data, smooth)
    for loc, res_group in data.groupby(res_key):
        parent_state = res_group['state_id'].values[0]
        try:
            parent_group = state_groups.get_group(parent_state)
            res_group = res_group.merge(parent_group, how="left",
                                        on="timestamp", suffixes=('', '_parent'))
            res_group = res_group.drop(columns=[res_key, "state_id", "state_id" + '_parent'])
        except KeyError:
            has_parent = False
            res_group = res_group.drop(columns=[res_key, "state_id"])
        res_group.set_index("timestamp", inplace=True)
        res_group = fill_dates(res_group, first_date, last_date)

        if smooth:
            if has_parent:
                if device:
                    stat, se, sample_size = smoothed_tests_per_device(
                        devices=res_group[f"numUniqueDevices_{suffix}"].values,
                        tests=res_group[f'totalTest_{suffix}'].values,
                        min_obs=MIN_OBS, pool_days=POOL_DAYS,
                        parent_devices=res_group[f"numUniqueDevices_{suffix}_parent"].values,
                        parent_tests=res_group[f"totalTest_{suffix}_parent"].values)
                else:
                    stat, se, sample_size = smoothed_positive_prop(
                        tests=res_group[f'totalTest_{suffix}'].values,
                        positives=res_group[f'positiveTest_{suffix}'].values,
                        min_obs=MIN_OBS, pool_days=POOL_DAYS,
                        parent_tests=res_group[f"totalTest_{suffix}_parent"].values,
                        parent_positives=res_group[f'positiveTest_{suffix}_parent'].values)
                    stat = stat * 100
            else:
                if device:
                    stat, se, sample_size = smoothed_tests_per_device(
                        devices=res_group[f"numUniqueDevices_{suffix}"].values,
                        tests=res_group[f'totalTest_{suffix}'].values,
                        min_obs=MIN_OBS, pool_days=POOL_DAYS)
                else:
                    stat, se, sample_size = smoothed_positive_prop(
                        tests=res_group[f'totalTest_{suffix}'].values,
                        positives=res_group[f'positiveTest_{suffix}'].values,
                        min_obs=MIN_OBS, pool_days=POOL_DAYS)
                    stat = stat * 100
        else:
            if device:
                stat, se, sample_size = raw_tests_per_device(
                    devices=res_group[f"numUniqueDevices_{suffix}"].values,
                    tests=res_group[f'totalTest_{suffix}'].values,
                    min_obs=MIN_OBS)
            else:
                stat, se, sample_size = raw_positive_prop(
                    tests=res_group[f'totalTest_{suffix}'].values,
                    positives=res_group[f'positiveTest_{suffix}'].values,
                    min_obs=MIN_OBS)
                stat = stat * 100

        se = se * 100
        res_df = pd.concat([
            res_df,
            pd.DataFrame({"geo_id": loc,
                         "timestamp": res_group.index,
                         "val": stat,
                         "se": se,
                         "sample_size": sample_size})
        ])
    return remove_null_samples(res_df)
