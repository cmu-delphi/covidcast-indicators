import pandas as pd
from datetime import datetime

from delphi_quidel_covidtest.generate_sensor import (MIN_OBS, POOL_DAYS,
                                                     generate_sensor_for_states,
                                                     generate_sensor_for_other_geores)

class TestGenerateSensor:
    def test_generate_sensor(self):
        
        # Test constants
        assert MIN_OBS > 0 
        assert isinstance(MIN_OBS, int)
        assert POOL_DAYS > 0
        assert isinstance(POOL_DAYS, int)
        
        # State Level 
        state_groups = pd.read_csv("./test_data/state_data.csv", sep = ",", 
                                 parse_dates=['timestamp']).groupby("state_id")

        # raw pct_positive
        state_pct_positive = generate_sensor_for_states(
            state_groups, smooth = False, device = False,
            first_date = datetime(2020, 6, 14), last_date = datetime(2020, 6, 20))

        assert (state_pct_positive.dropna()["val"] < 100).all()
        assert set(state_pct_positive.columns) == set(["geo_id", "val", "se", "sample_size", "timestamp"])
        assert len(state_pct_positive.groupby("geo_id").count()["timestamp"].unique()) == 1
        
        # raw test_per_device
        state_test_per_device = generate_sensor_for_states(
            state_groups, smooth = False, device = True,
            first_date = datetime(2020, 6, 14), last_date = datetime(2020, 6, 20))

        assert state_test_per_device["se"].isnull().all()
        assert set(state_test_per_device.columns) == set(["geo_id", "val", "se", "sample_size", "timestamp"])
        assert len(state_test_per_device.groupby("geo_id").count()["timestamp"].unique()) == 1
        
        
        # MSA level
        # smoothed pct_positive
        msa_data = pd.read_csv("./test_data/msa_data.csv", sep = ",", 
                                 parse_dates=['timestamp'])
        msa_pct_positive = generate_sensor_for_other_geores(
            state_groups, msa_data, "cbsa_id", smooth = True, device = False,
            first_date = datetime(2020, 6, 14), last_date = datetime(2020, 6, 20))
        
        assert (msa_pct_positive.dropna()["val"] < 100).all()
        assert set(msa_pct_positive.columns) == set(["geo_id", "val", "se", "sample_size", "timestamp"])
        assert len(msa_pct_positive.groupby("geo_id").count()["timestamp"].unique()) == 1
        
        # smoothed test_per_device
        msa_test_per_device = generate_sensor_for_other_geores(
            state_groups, msa_data, "cbsa_id", smooth = True, device = True,
            first_date = datetime(2020, 6, 14), last_date = datetime(2020, 6, 20))
        
        assert msa_test_per_device["se"].isnull().all()     
        assert set(msa_test_per_device.columns) == set(["geo_id", "val", "se", "sample_size", "timestamp"])
        assert len(msa_test_per_device.groupby("geo_id").count()["timestamp"].unique()) == 1
        
