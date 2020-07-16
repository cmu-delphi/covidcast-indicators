import pytest

from os.path import join

import numpy as np
import pandas as pd
from datetime import datetime

from delphi_quidel_covidtest.generate_sensor import (generate_sensor_for_states,
                                            generate_sensor_for_other_geores)

class TestGenerateSensor:
    def test_generate_sensor(self):
        
        # State Level
        state_data = pd.read_csv("./test_data/state_data.csv", sep = ",", 
                                 parse_dates=['timestamp'])
        raw_state_df, state_groups = generate_sensor_for_states(state_data, smooth = False,
                                                                first_date = datetime(2020, 6, 10),
                                                                last_date = datetime(2020, 6, 20))

        assert (raw_state_df.dropna()["val"] < 100).all
        assert (raw_state_df.columns == ["geo_id", "val", "se", "sample_size", "timestamp"]).all()
        assert len(raw_state_df.groupby("geo_id").count()["timestamp"].unique()) == 1
        
        # MSA level   
        msa_data = pd.read_csv("./test_data/msa_data.csv", sep = ",", 
                                 parse_dates=['timestamp'])
        raw_msa_df = generate_sensor_for_other_geores(state_groups, msa_data, "cbsa_id", 
                                                      smooth = True,
                                                      first_date = datetime(2020, 6, 10),
                                                      last_date = datetime(2020, 6, 20))
        assert (raw_msa_df.dropna()["val"] < 100).all
        assert (raw_msa_df.columns == ["geo_id", "val", "se", "sample_size", "timestamp"]).all()
        assert len(raw_msa_df.groupby("geo_id").count()["timestamp"].unique()) == 1
        
