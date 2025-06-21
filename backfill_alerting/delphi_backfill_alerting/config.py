"""
This file contains configuration variables used for the backfill alerting.
"""

from datetime import datetime, timedelta

class Config:
    """Static configuration variables."""

    ## dates
    FIRST_DATA_DATE = datetime(2020, 1, 1)

    # shift dates forward for labeling purposes
    DAY_SHIFT = timedelta(days=1)

    ## data columns
    COVID_COUNT = "Covid"
    TOTAL_COUNT = "Denom"
    COUNT_COL = "count"
    DATE_COL = "time_value"
    GEO_COL = "geo_value"
    ID_COLS = [DATE_COL] + [GEO_COL]

    DATA_COLS = [DATE_COL, GEO_COL, COUNT_COL]
    DATA_DTYPES = {DATE_COL: str, COUNT_COL: str, GEO_COL: str}

    COUNT_TYPES = [COVID_COUNT, TOTAL_COUNT]

    ## file path
    FILE_PATH = "%s/%s_Counts_Products_%s.dat.gz"

    ## GEO RELATED
    COUNTY_LEVEL = "fips"
    STATE_LEVEL = "state_id"
    GEO_LEVELS = [COUNTY_LEVEL, STATE_LEVEL]

    # Backfill Variables
    CHANGE_RATE = "cr"
    BACKFILL_FRACTION = "frc"
    BACKFILL_VARS = [CHANGE_RATE, BACKFILL_FRACTION]
    BACKFILL_REF_LAG = {CHANGE_RATE: [1, 7],
                        BACKFILL_FRACTION: [60]}

    # Training variable
    LAG_SPLITS = list(range(-1, 15)) + [28, 42, 60]

    # For Alerting Messages
    bv_names = {("cr", 7): "7-day change rate",
                ("cr", 1): "Daily change rate",
                ("frc", 60): "Backfill Fraction (anchor=60)"}
    count_names = {"Covid": "COVID counts", "Denom": "Total counts"}
    geo_names = {"fips": "county", "state_id": "state"}
