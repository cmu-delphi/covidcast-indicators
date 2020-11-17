"""
Sensorizer class to convert an indicator into a sensor.
Uses univariate linear regression from indicator to target.

Author: Aaron Rumack
Created: 2020-11-11

"""

import logging

# third party
import numpy as np
import pandas as pd
from sklearn import linear_model
from datetime import timedelta

# first party
from .config import Config

class Sensorizer:
    """Sensorizer class to convert indicator into a sensor.
    """

    @staticmethod
    def sensorize(
            signal,
            target,
            signal_geo_col,
            signal_time_col,
            signal_val_col,
            target_geo_col,
            target_time_col,
            target_val_col,
            window_start=Config.SENSOR_WINDOW_START,
            window_end=Config.SENSOR_WINDOW_END):
        """
        Sensorize a signal to correct for spatial heterogeneity. For each
         date, use linear regression to fit target to signal globally (f) and
         fit signal to target locally (g_i). Output sensorized signal as
         f(g_i(signal)).

        Args:
            signal: DataFrame with signal values to be sensorized
            target: DataFrame with target observations for sensor fit
            {signal,target}_geo_col: name of location column in DataFrame
            {signal,target}_time_col: name of time column in DataFrame
            {signal,target}_val_col: name of value column in DataFrame
            window_start: Number of days before fit date at which to begin
                          training, inclusive
                          Positive integer
            window_end: Number of days before fit date at which to end training,
                        exclusive
                        Positive integer, greater than window_start

        Returns:
            DataFrame with signal_geo_col, signal_time_col, and signal_val_col
            Values in signal_val_col are sensorized

        """

        logging.info("Sensorizing")
        signal = signal.sort_values([signal_time_col, signal_geo_col])
        target = target.sort_values([target_time_col, target_geo_col])
        target = target.rename(columns={
                                target_time_col:signal_time_col,
                                target_geo_col:signal_geo_col,
                                target_val_col:"target"})
        signal = signal.rename(columns={signal_val_col:"signal"})
        merged = pd.merge(signal,target,on=[signal_time_col,signal_geo_col],how="left")

        unique_times = pd.to_datetime(merged[signal_time_col].unique())
        global_fit_df = pd.DataFrame(data={signal_time_col:unique_times})
        global_fit_df["global_b1"] = 0
        global_fit_df["global_b0"] = 0
        for i, t in enumerate(unique_times):
            train_start = t - timedelta(days=window_start)
            train_end = t - timedelta(days=window_end)
            train_data = merged[
                (merged[signal_time_col] <= train_start) & (merged[signal_time_col] > train_end)
            ]

            xs = train_data["target"].values
            ys = train_data["signal"].values
            if len(xs) >= 2:
                mx = np.nanmean(xs)
                my = np.nanmean(ys)
                b1 = np.nansum((xs-mx)*(ys-my)) / np.nansum(np.power(xs-mx,2))
                global_fit_df.loc[i,"global_b1"] = b1
                global_fit_df.loc[i,"global_b0"] = my - mx*b1
            else:
                # Can't fit coefficients, zero/insufficient training data
                global_fit_df.loc[i,"global_b1"] = np.nan
                global_fit_df.loc[i,"global_b0"] = np.nan
            
        merged_by_geo = merged.groupby(signal_geo_col)
        local_fit_df = signal.copy()[[signal_time_col,signal_geo_col]].reset_index()
        local_fit_df["local_b1"] = 0
        local_fit_df["local_b0"] = 0
        for i in range(len(local_fit_df.index)):
            t = local_fit_df.loc[i,signal_time_col]
            train_start = t - timedelta(days=window_start)
            train_end = t - timedelta(days=window_end)
            geo = local_fit_df.loc[i,signal_geo_col]
            train_data = merged_by_geo.get_group(geo)
            train_data = train_data[
                (train_data[signal_time_col] <= train_start) & (train_data[signal_time_col] > train_end)
            ]
            xs = train_data["signal"].values
            ys = train_data["target"].values
            if len(xs) >= 2:
                mx = np.nanmean(xs)
                my = np.nanmean(ys)
                b1 = np.nansum((xs-mx)*(ys-my)) / np.nansum(np.power(xs-mx,2))
                local_fit_df.loc[i,"local_b1"] = b1                         
                local_fit_df.loc[i,"local_b0"] = my - mx*b1                 
            else:
                # Can't fit coefficients, zero/insufficient training data
                local_fit_df.loc[i,"local_b1"] = np.nan
                local_fit_df.loc[i,"local_b0"] = np.nan

        combined_df = pd.merge(merged,global_fit_df,on=[signal_time_col],how="left")
        combined_df = pd.merge(combined_df,local_fit_df,on=[signal_time_col,signal_geo_col],how="left")
        
        # First, calculate estimate in target space
        combined_df["sensor"] = combined_df["signal"]*combined_df["local_b1"] + combined_df["local_b0"]
        # Second, scale back into signal space
        combined_df["sensor"] = combined_df["sensor"]*combined_df["global_b1"] + combined_df["global_b0"]
        # Where we could not fit regression coefficients, use original signal
        combined_df.sensor = combined_df["sensor"].fillna(combined_df["signal"])

        combined_df.to_csv("combined_df.csv",index=False)

        result = combined_df[[signal_geo_col,signal_time_col,"sensor"]]
        result = result.rename(columns={"sensor":signal_val_col})
        return result

        
