"""
This file contains configuration variables used to generate the doctor visits signal.

Author: Maria
Created: 2020-04-16
Last modified: 2020-06-17
"""

from datetime import datetime, timedelta


class Config:
    """Static configuration variables.
    """

    # dates
    FIRST_DATA_DATE = datetime(2020, 1, 1)
    DAY_SHIFT = timedelta(days=1)  # shift dates forward for labeling purposes

    # data columns
    CLI_COLS = ["Covid_like", "Flu_like", "Mixed"]
    FLU1_COL = ["Flu1"]
    COUNT_COLS = CLI_COLS + FLU1_COL + ["Denominator"]
    DATE_COL = "ServiceDate" #"servicedate"
    GEO_COL = "PatCountyFIPS" #"patCountyFIPS"
    AGE_COL = "PatAgeGroup" #"patAgeGroup"
    HRR_COLS = ["Pat HRR Name", "Pat HRR ID"]#["patHRRname", "patHRRid"]
    ID_COLS = [DATE_COL] + [GEO_COL] + [AGE_COL] + HRR_COLS
    FILT_COLS = ID_COLS + COUNT_COLS
    DTYPES = {DATE_COL: str, GEO_COL: str,
              "Denominator": int, "Flu1": int,
              "Covid_like": int, "Flu_like": int,
              "Mixed": int, AGE_COL: str,
              HRR_COLS[0]: str, HRR_COLS[1]: float}

    SMOOTHER_BANDWIDTH = 100  # bandwidth for the linear left Gaussian filter
    MAX_BACKFILL_WINDOW = 7  # maximum number of days used to average a backfill correction
    MIN_CUM_VISITS = 500  # need to observe at least 500 counts before averaging
    RECENT_LENGTH = 7  # number of days to sum over for sparsity threshold
    MIN_RECENT_VISITS = 100  # min numbers of visits needed to include estimate
    MIN_RECENT_OBS = 3  # minimum days needed to produce an estimate for latest time

    SENSOR_WINDOW_START = None # 7 # start of training window for sensorization
    SENSOR_WINDOW_END = 42 # end of training window for sensorization
