# standard
import math

# Third party
import numpy as np
import numpy.random as nr
import pandas as pd
# first party
from delphi_backfill_alerting.config import Config
from delphi_backfill_alerting.data_tools import *
from delphi_backfill_alerting.backfill import *
from delphi_backfill_alerting.model import *

PARAMS = {
    "indicator": {
        "result_cache_dir": "test_data/results",
        "data_cache_dir": "test_data/data",
        "types": ["covid", "total"],
        "drop_date": "2021-01-01"
    }
}
DROP_DATE = pd.to_datetime(PARAMS["indicator"]["drop_date"])
RESULT_CACHE_DIR = PARAMS["indicator"]["result_cache_dir"]
DATA_CACHE_DIR = PARAMS["indicator"]["data_cache_dir"]
STATE_DATA = "state_data.csv"

class TestModel:
    
    # Test data for 7-day change rate 
    state_df = pd.read_csv("/".join([DATA_CACHE_DIR, STATE_DATA]), 
                           parse_dates=["time_value", "issue_date"])
    traindf, testdf = model_traning_and_testing(
            state_df, DROP_DATE, Config.CHANGE_RATE, 7)
    alerts = evaluation([traindf, testdf], DROP_DATE, RESULT_CACHE_DIR, test=True)
    
    def test_model_training_and_testing(self):
        
        assert "predicted" in self.traindf.columns
        assert "predicted" in self.testdf.columns

    def evaluation(self):
        assert len(self.alerts) == 4
    
   