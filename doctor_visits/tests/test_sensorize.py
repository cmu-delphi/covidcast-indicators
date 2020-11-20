import pytest

# third party
import numpy as np
import pandas as pd
from datetime import timedelta

# first party
from delphi_doctor_visits.sensorize import Sensorizer

class TestSensorizer:

    def test_regression(self):
        toy_grouped = pd.DataFrame(data={
            "geo":['1','1','1','1','2','2','2','2'],
            "time":['1','2','3','4','1','2','3','4'],
            "signal":[1,2,3,4,11,12,13,14],
            "target":[1,2,3,4,1,2,3,4]})
        toy_grouped = toy_grouped.groupby("geo")
        coef_df = Sensorizer.linear_regression_coefs(toy_grouped)

        coefs_b1 = np.array([1,1])
        coefs_b0 = np.array([0,-10])
        assert np.allclose(coef_df["b1"].values, coefs_b1)
        assert np.allclose(coef_df["b0"].values, coefs_b0)

        coef_df = Sensorizer.linear_regression_coefs(
                    toy_grouped,fit_intercept=False)
        coefs_b1 = np.array([1,0.20634920634920634])
        coefs_b0 = np.array([0,0])
        assert np.allclose(coef_df["b1"].values, coefs_b1)
        assert np.allclose(coef_df["b0"].values, coefs_b0)


    def test_sensorize(self):
        toy_df = pd.DataFrame(data={
            "geo":["a","a","a","a","a","b","b","b","b","b"],
            "time":pd.to_datetime(np.tile(pd.date_range("2020-07-01","2020-07-05"),2)),
            "signal":np.array([1,2,3,5,5,2,3,3,5,6])*0.01,
            "target":np.array([1,2,3,4,5,2,3,4,5,6])*0.01})
        signal_df = toy_df[["geo","time","signal"]]
        target_df = toy_df[["geo","time","target"]]

        coef_df = Sensorizer.sensorize(signal_df,target_df,
            "geo","time","signal","geo","time","target",
            window_start=1,window_end=3)

        local_b1 = np.array([np.nan,np.nan,1,1,0.5,np.nan,np.nan,1,np.nan,0.5])
        local_b0 = np.array([np.nan,np.nan,0,0,1.5,np.nan,np.nan,0,np.nan,2.5])*0.01
        global_b1 = np.tile(np.array([np.nan,np.nan,1,0.5,2]),2)
        global_b0 = np.tile(np.array([np.nan,np.nan,0,1.25,-4])*0.01,2)
        sensor_values = global_b1*(local_b1*signal_df.signal.values+local_b0)+global_b0
        sensor_values[np.isnan(sensor_values)] = signal_df.signal.values[np.isnan(sensor_values)]

        coef_df.to_csv("coef_df.csv")
        assert np.allclose(sensor_values, coef_df.signal.values)
