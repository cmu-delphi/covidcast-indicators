# -*- coding: utf-8 -*-
"""
Functions to help generate sensor for different geographical levels
"""
import pandas as pd
from .data_tools import fill_dates, raw_positive_prop, smoothed_positive_prop

MIN_OBS = 50  # minimum number of observations in order to compute a proportion.
POOL_DAYS = 7

def generate_sensor_for_states(state_data, smooth):
    """ 
    fit over states 
    Args:
        state_data: pd.DataFrame
        state_key: "state_id"
        smooth: bool
    Output:
        df: pd.DataFrame    
    """
    state_df = pd.DataFrame(columns = ["geo_id", "val", "se", "sample_size", "timestamp"])
    state_groups = state_data.groupby("state_id")
    state_list = list(state_groups.groups.keys())
    for state in state_list:
        state_group = state_groups.get_group(state)
        state_group = state_group.drop(columns=["state_id"])
        state_group.set_index("timestamp", inplace=True)
        state_group = fill_dates(state_group)
    
    
        fit_func = smoothed_positive_prop if smooth else raw_positive_prop
        stat, se, sample_size = fit_func(tests=state_group['totalTest'].values,
                                          positives=state_group['positiveTest'].values,
                                          min_obs=MIN_OBS, pool_days=POOL_DAYS)
        stat = stat * 100
        se = se * 100
        state_df = state_df.append(pd.DataFrame({"geo_id": state,
                                                 "timestamp": state_group.index,
                                                 "val": stat,
                                                 "se": se,
                                                 "sample_size": sample_size}))
    return state_df, state_groups

def generate_sensor_for_other_geores(state_groups, data, res_key, smooth):
    """
    fit over counties/HRRs/MSAs 
    Args:
        data: pd.DataFrame
        res_key: "fips", "cbsa_id" or "hrrnum"
        smooth: bool
    Output:
        df: pd.DataFrame  
    """
    res_df = pd.DataFrame(columns = ["geo_id", "val", "se", "sample_size"])
    res_groups = data.groupby(res_key)
    loc_list = list(res_groups.groups.keys())
    for loc in loc_list:
        res_group = res_groups.get_group(loc)
        parent_state = res_group['state_id'].values[0]
        parent_group = state_groups.get_group(parent_state)
        res_group = res_group.merge(parent_group, how="left",
                                    on="timestamp", suffixes=('', '_parent'))
        res_group = res_group.drop(columns=[res_key, "state_id", "state_id" + '_parent'])
        res_group.set_index("timestamp", inplace=True)
        res_group = fill_dates(res_group)

        if smooth:
            stat, se, sample_size = smoothed_positive_prop(
                tests=res_group['totalTest'].values,
                positives=res_group['positiveTest'].values,
                min_obs=MIN_OBS, pool_days=POOL_DAYS,
                parent_tests=res_group["totalTest_parent"].values,
                parent_positives=res_group['positiveTest_parent'].values)
        else:
            stat, se, sample_size = raw_positive_prop(
                tests=res_group['totalTest'].values,
                positives=res_group['positiveTest'].values,
                min_obs=MIN_OBS, pool_days=POOL_DAYS)
        stat = stat * 100
        se = se * 100
        
        res_df = res_df.append(pd.DataFrame({"geo_id": loc,
                                             "timestamp": res_group.index,
                                             "val": stat,
                                             "se": se,
                                             "sample_size": sample_size}))
    
    return res_df

